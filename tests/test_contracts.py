from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipeline.contracts import ContractError, FieldSpec, load_contract


def valid_row() -> dict:
    return {
        "city_id": "berlin",
        "latitude": 52.52,
        "longitude": 13.405,
        "observed_at_utc": datetime(2026, 8, 20, 12, tzinfo=timezone.utc),
        "temperature_2m_c": 21.4,
        "relative_humidity_2m_pct": 55.0,
        "precipitation_mm": 0.0,
        "wind_speed_10m_kmh": 9.0,
        "weather_code": 3,
    }


def test_contract_loads_from_yaml(contract):
    assert contract.name == "weather_observation"
    assert contract.version == 1
    assert contract.primary_key == ("city_id", "observed_at_utc")
    assert contract.source_lag_days == 2


def test_valid_row_has_no_violations(contract):
    assert contract.validate_row(valid_row()) == []


def test_missing_required_field_is_a_violation(contract):
    row = valid_row() | {"city_id": None}

    problems = contract.validate_row(row)

    assert any("required field is null" in p for p in problems)


def test_nullable_field_may_be_null(contract):
    assert contract.validate_row(valid_row() | {"temperature_2m_c": None}) == []


def test_out_of_range_value_is_a_violation(contract):
    problems = contract.validate_row(valid_row() | {"relative_humidity_2m_pct": 140.0})

    assert any("above contract maximum" in p for p in problems)


def test_wrong_type_is_a_violation(contract):
    problems = contract.validate_row(valid_row() | {"temperature_2m_c": "21.4"})

    assert any("expected float, got str" in p for p in problems)


def test_split_valid_partitions_the_batch(contract):
    rows = [valid_row(), valid_row() | {"latitude": 999.0}]

    accepted, rejected = contract.split_valid(rows)

    assert len(accepted) == 1
    assert len(rejected) == 1
    assert "latitude" in rejected[0][1][0]


def test_unknown_fields_are_reported(contract):
    assert contract.unknown_fields(valid_row() | {"uv_index": 4.0}) == {"uv_index"}


def test_missing_contract_file_raises():
    with pytest.raises(ContractError, match="not found"):
        load_contract("weather_observation", 99)


def test_field_spec_rejects_unknown_type():
    with pytest.raises(ContractError, match="unknown type"):
        FieldSpec(name="x", type="geography", required=True).violations("POINT(0 0)")
