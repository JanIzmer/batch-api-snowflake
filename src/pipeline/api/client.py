"""HTTP client for the Open-Meteo archive API.

Design notes
------------
* Retries are only applied to *transient* failures (timeouts, connection
  errors, 429, 5xx). A 4xx other than 429 means we sent a bad request; retrying
  it just burns the rate limit and delays the alert.
* `Retry-After` is honoured when the server sends it, otherwise exponential
  backoff with jitter so a fleet of tasks does not retry in lockstep.
* The client is deliberately dumb about dates: the caller decides the window,
  which is what makes backfill a matter of calling it in a loop.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import httpx
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from pipeline.logging_conf import get_logger
from pipeline.models import City

log = get_logger(__name__)

HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
)


class ApiError(RuntimeError):
    """Base class for upstream failures."""


class TransientApiError(ApiError):
    """Worth retrying: timeout, connection reset, 429, 5xx."""


class PermanentApiError(ApiError):
    """Not worth retrying: 4xx caused by our own request."""


def _log_retry(state: RetryCallState) -> None:
    log.warning(
        "api.retry",
        attempt=state.attempt_number,
        sleep_seconds=round(getattr(state.next_action, "sleep", 0.0), 2),
        error=str(state.outcome.exception()) if state.outcome else None,
    )


class WeatherApiClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 30.0,
        max_retries: int = 5,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(timeout_seconds),
            headers={"User-Agent": "batch-api-snowflake/0.1 (+data-engineering)"},
            # Keep the pool small: we are polite to a free API and the
            # concurrency we want comes from Airflow, not from the client.
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
        )

    def __enter__(self) -> WeatherApiClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def fetch_hourly(self, city: City, start: date, end: date) -> dict[str, Any]:
        """Fetch the hourly archive for one city over an inclusive date range."""
        if end < start:
            raise ValueError(f"end {end} is before start {start}")

        params = {
            "latitude": city.latitude,
            "longitude": city.longitude,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "UTC",
        }
        payload = self._get("/archive", params)
        log.info(
            "api.fetched",
            city_id=city.city_id,
            start=start.isoformat(),
            end=end.isoformat(),
            hours=len((payload.get("hourly") or {}).get("time") or []),
        )
        return payload

    # -- internals ---------------------------------------------------------

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        # Build the retry decorator here so max_retries stays instance level.
        wrapped = retry(
            retry=retry_if_exception_type(TransientApiError),
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential_jitter(initial=1, max=60),
            before_sleep=_log_retry,
            reraise=True,
        )(self._get_once)
        return wrapped(path, params)

    def _get_once(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            response = self._client.get(url, params=params)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise TransientApiError(f"transport failure calling {url}: {exc}") from exc

        if response.status_code == httpx.codes.TOO_MANY_REQUESTS:
            raise TransientApiError(
                f"rate limited by {url}; retry-after={response.headers.get('Retry-After')}"
            )
        if response.status_code >= 500:
            raise TransientApiError(f"{url} returned {response.status_code}")
        if response.status_code >= 400:
            raise PermanentApiError(f"{url} returned {response.status_code}: {response.text[:500]}")

        try:
            return response.json()
        except ValueError as exc:
            # A 200 with a non-JSON body is usually a proxy/captive portal, so
            # it is transient far more often than it is a real API change.
            raise TransientApiError(f"{url} returned 200 with a non-JSON body") from exc
