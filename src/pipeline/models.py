"""Domain models.

The API returns a columnar payload (parallel arrays under `hourly`). These
models flatten it into one row per (city, hour) and attach the metadata columns
the warehouse needs for idempotency.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

CONTRACT_VERSION = 1


class City(BaseModel):
    """A location we pull. The catalogue is ours, the API has no city concept."""

    city_id: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str
    country_code: str = Field(min_length=2, max_length=2)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    timezone_name: str = "UTC"


class WeatherObservation(BaseModel):
    """One row of the RAW table. Field names match the contract exactly."""

    city_id: str
    latitude: float
    longitude: float
    observed_at_utc: datetime
    temperature_2m_c: float | None = None
    relative_humidity_2m_pct: float | None = None
    precipitation_mm: float | None = None
    wind_speed_10m_kmh: float | None = None
    weather_code: int | None = None

    @field_validator("observed_at_utc")
    @classmethod
    def _must_be_utc(cls, value: datetime) -> datetime:
        """Naive timestamps from the API are UTC by construction; make it explicit."""
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @property
    def observation_date(self) -> date:
        return self.observed_at_utc.date()


def batch_id(city_id: str, observation_date: date, contract_version: int = CONTRACT_VERSION) -> str:
    """Deterministic batch id.

    Re-running the same (city, day) produces the same id, which is what makes
    the MERGE in Snowflake idempotent and lets us delete a bad batch precisely.
    """
    key = f"{city_id}|{observation_date.isoformat()}|v{contract_version}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def payload_hash(payload: dict[str, Any]) -> str:
    """Stable hash of the raw API payload, used to skip unchanged reloads."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


def flatten_hourly(city: City, payload: dict[str, Any]) -> list[WeatherObservation]:
    """Turn the columnar `hourly` block into row-per-hour observations.

    The API guarantees every array under `hourly` has the same length as `time`.
    We do not trust that: a short array would otherwise silently shift values
    onto the wrong hour, which is far worse than a loud failure.
    """
    hourly = payload.get("hourly") or {}
    times: list[str] = hourly.get("time") or []
    if not times:
        return []

    mapping = {
        "temperature_2m_c": "temperature_2m",
        "relative_humidity_2m_pct": "relative_humidity_2m",
        "precipitation_mm": "precipitation",
        "wind_speed_10m_kmh": "wind_speed_10m",
        "weather_code": "weather_code",
    }
    for source in mapping.values():
        series = hourly.get(source)
        if series is not None and len(series) != len(times):
            raise ValueError(
                f"{city.city_id}: series '{source}' has {len(series)} points "
                f"but 'time' has {len(times)}"
            )

    rows: list[WeatherObservation] = []
    for index, raw_time in enumerate(times):
        values: dict[str, Any] = {
            target: (hourly.get(source) or [None] * len(times))[index]
            for target, source in mapping.items()
        }
        rows.append(
            WeatherObservation(
                city_id=city.city_id,
                latitude=float(payload.get("latitude", city.latitude)),
                longitude=float(payload.get("longitude", city.longitude)),
                observed_at_utc=datetime.fromisoformat(raw_time),
                **values,
            )
        )
    return rows
