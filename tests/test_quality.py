from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from pipeline.quality import DataQualityError, check_batch


def row(**overrides):
    base = {
        "city_id": "berlin",
        "latitude": 52.52,
        "longitude": 13.405,
        "observed_at_utc": datetime(2026, 8, 20, 12, tzinfo=UTC),
        "temperature_2m_c": 20.0,
        "relative_humidity_2m_pct": 50.0,
        "precipitation_mm": 0.0,
        "wind_speed_10m_kmh": 10.0,
        "weather_code": 1,
    }
    return base | overrides


def test_clean_batch_passes(contract, tmp_path, day):
    accepted, report = check_batch(contract, [row()] * 24, "berlin", day, tmp_path)

    assert len(accepted) == 24
    assert report.rejected_rows == 0
    assert report.quarantine_path is None


def test_a_few_bad_rows_are_quarantined_not_fatal(contract, tmp_path, day):
    rows = [row() for _ in range(200)]
    rows[7] = row(relative_humidity_2m_pct=250.0)

    accepted, report = check_batch(contract, rows, "berlin", day, tmp_path)

    assert len(accepted) == 199
    assert report.rejected_rows == 1
    assert report.quarantine_path is not None

    quarantined = [json.loads(line) for line in report.quarantine_path.read_text().splitlines()]
    assert len(quarantined) == 1
    assert "relative_humidity_2m_pct" in quarantined[0]["_reasons"][0]


def test_too_many_bad_rows_fail_the_batch(contract, tmp_path, day):
    """A high reject ratio means the producer changed, not that a sensor blipped."""
    rows = [row() for _ in range(10)]
    rows[0] = row(latitude=None)
    rows[1] = row(latitude=None)

    with pytest.raises(DataQualityError, match="above the"):
        check_batch(contract, rows, "berlin", day, tmp_path)


def test_threshold_is_configurable(contract, tmp_path, day):
    rows = [row() for _ in range(10)]
    rows[0] = row(latitude=None)

    _, report = check_batch(contract, rows, "berlin", day, tmp_path, max_reject_ratio=0.5)

    assert report.reject_ratio == pytest.approx(0.1)


def test_unknown_producer_fields_are_reported(contract, tmp_path, day):
    _, report = check_batch(contract, [row(uv_index=4.0)], "berlin", day, tmp_path)

    assert report.unknown_fields == ("uv_index",)


def test_empty_batch_has_zero_ratio(contract, tmp_path, day):
    accepted, report = check_batch(contract, [], "berlin", day, tmp_path)

    assert accepted == []
    assert report.reject_ratio == 0.0
