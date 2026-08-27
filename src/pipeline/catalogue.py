"""The city catalogue.

Read from the same CSV dbt seeds, so ingestion and modelling can never disagree
about which cities exist.
"""

from __future__ import annotations

import csv
from pathlib import Path

from pipeline.models import City

DEFAULT_CATALOGUE = Path("seeds/city_catalogue.csv")


def load_cities(path: Path | str = DEFAULT_CATALOGUE, only: list[str] | None = None) -> list[City]:
    """Load the catalogue, optionally filtered to a subset of city ids."""
    catalogue = Path(path)
    if not catalogue.exists():
        raise FileNotFoundError(f"city catalogue not found: {catalogue}")

    with catalogue.open(newline="", encoding="utf-8") as handle:
        cities = [
            City(
                city_id=row["city_id"],
                name=row["name"],
                country_code=row["country_code"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                timezone_name=row["timezone_name"],
            )
            for row in csv.DictReader(handle)
        ]

    if only:
        wanted = set(only)
        missing = wanted - {c.city_id for c in cities}
        if missing:
            raise ValueError(f"unknown city ids: {sorted(missing)}")
        cities = [c for c in cities if c.city_id in wanted]

    return cities
