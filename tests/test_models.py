from __future__ import annotations

from datetime import date, timezone

import pytest

from pipeline.models import batch_id, flatten_hourly, payload_hash
from tests.conftest import hourly_payload


def test_flatten_produces_one_row_per_hour(berlin, payload):
    rows = flatten_hourly(berlin, payload)

    assert len(rows) == 24
    assert rows[0].city_id == "berlin"
    assert rows[0].observed_at_utc.tzinfo == timezone.utc
    assert rows[0].observation_date == date(2026, 8, 20)


def test_flatten_rejects_ragged_series(berlin):
    """A short series would silently shift values onto the wrong hour."""
    payload = hourly_payload(temperature_2m=[18.0, 19.0])

    with pytest.raises(ValueError, match="temperature_2m"):
        flatten_hourly(berlin, payload)


def test_flatten_keeps_nulls_as_nulls(berlin):
    payload = hourly_payload()
    payload["hourly"]["temperature_2m"][5] = None

    rows = flatten_hourly(berlin, payload)

    assert rows[5].temperature_2m_c is None
    assert rows[6].temperature_2m_c is not None


def test_flatten_handles_empty_window(berlin):
    assert flatten_hourly(berlin, {"hourly": {"time": []}}) == []


def test_batch_id_is_deterministic():
    first = batch_id("berlin", date(2026, 8, 20))
    second = batch_id("berlin", date(2026, 8, 20))

    assert first == second
    assert first != batch_id("berlin", date(2026, 8, 21))
    assert first != batch_id("warsaw", date(2026, 8, 20))


def test_payload_hash_ignores_key_order():
    assert payload_hash({"a": 1, "b": 2}) == payload_hash({"b": 2, "a": 1})


def test_payload_hash_changes_when_values_change():
    assert payload_hash({"a": 1}) != payload_hash({"a": 2})
