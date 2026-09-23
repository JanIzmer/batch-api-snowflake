from __future__ import annotations

from datetime import date

import httpx
import pytest
import respx

from pipeline.api.client import (
    PermanentApiError,
    TransientApiError,
    WeatherApiClient,
)
from tests.conftest import hourly_payload

BASE_URL = "https://api.test/v1"
ARCHIVE = f"{BASE_URL}/archive"


def make_client(**kwargs) -> WeatherApiClient:
    # No jitter wait in tests would still sleep; keep retries low instead.
    return WeatherApiClient(base_url=BASE_URL, timeout_seconds=1.0, **kwargs)


@respx.mock
def test_successful_fetch_returns_payload(berlin, day):
    route = respx.get(ARCHIVE).mock(return_value=httpx.Response(200, json=hourly_payload()))

    with make_client() as client:
        payload = client.fetch_hourly(berlin, day, day)

    assert len(payload["hourly"]["time"]) == 24
    assert route.call_count == 1
    request = route.calls[0].request
    assert request.url.params["start_date"] == "2026-08-20"
    assert request.url.params["timezone"] == "UTC"


@respx.mock
def test_server_error_is_retried_then_succeeds(berlin, day):
    route = respx.get(ARCHIVE).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(200, json=hourly_payload()),
        ]
    )

    with make_client(max_retries=3) as client:
        payload = client.fetch_hourly(berlin, day, day)

    assert route.call_count == 2
    assert payload["hourly"]["time"]


@respx.mock
def test_rate_limit_is_treated_as_transient(berlin, day):
    respx.get(ARCHIVE).mock(return_value=httpx.Response(429, headers={"Retry-After": "1"}))

    with (
        make_client(max_retries=2) as client,
        pytest.raises(TransientApiError, match="rate limited"),
    ):
        client.fetch_hourly(berlin, day, day)


@respx.mock
def test_client_error_is_not_retried(berlin, day):
    """A 400 means our request is wrong; retrying only delays the alert."""
    route = respx.get(ARCHIVE).mock(return_value=httpx.Response(400, text="bad latitude"))

    with make_client(max_retries=5) as client, pytest.raises(PermanentApiError):
        client.fetch_hourly(berlin, day, day)

    assert route.call_count == 1


@respx.mock
def test_timeout_is_retried_and_finally_raises(berlin, day):
    route = respx.get(ARCHIVE).mock(side_effect=httpx.ConnectTimeout("too slow"))

    with make_client(max_retries=2) as client, pytest.raises(TransientApiError):
        client.fetch_hourly(berlin, day, day)

    assert route.call_count == 2


@respx.mock
def test_non_json_200_is_transient(berlin, day):
    respx.get(ARCHIVE).mock(return_value=httpx.Response(200, text="<html>portal</html>"))

    with make_client(max_retries=1) as client, pytest.raises(TransientApiError, match="non-JSON"):
        client.fetch_hourly(berlin, day, day)


def test_inverted_range_is_rejected_before_any_request(berlin):
    with make_client() as client, pytest.raises(ValueError, match="before start"):
        client.fetch_hourly(berlin, date(2026, 8, 20), date(2026, 8, 19))
