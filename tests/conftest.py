from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from pipeline.contracts import Contract, load_contract
from pipeline.models import City

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def contract() -> Contract:
    return load_contract("weather_observation", 1, REPO_ROOT / "contracts")


@pytest.fixture
def berlin() -> City:
    return City(
        city_id="berlin",
        name="Berlin",
        country_code="DE",
        latitude=52.52,
        longitude=13.405,
        timezone_name="Europe/Berlin",
    )


@pytest.fixture
def day() -> date:
    return date(2026, 8, 20)


def hourly_payload(hours: int = 24, **overrides: Any) -> dict[str, Any]:
    """A miniature version of the real API response."""
    times = [f"2026-08-20T{hour:02d}:00" for hour in range(hours)]
    payload: dict[str, Any] = {
        "latitude": 52.52,
        "longitude": 13.405,
        "timezone": "GMT",
        "hourly": {
            "time": times,
            "temperature_2m": [18.0 + i * 0.1 for i in range(hours)],
            "relative_humidity_2m": [60.0] * hours,
            "precipitation": [0.0] * hours,
            "wind_speed_10m": [12.0] * hours,
            "weather_code": [3] * hours,
        },
    }
    payload["hourly"].update(overrides)
    return payload


@pytest.fixture
def payload() -> dict[str, Any]:
    return hourly_payload()
