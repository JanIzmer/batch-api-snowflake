"""The ingestion run itself: fetch -> validate -> land -> load.

The unit of work is one (city, day). Everything above it - a day for all
cities, a month long backfill - is a loop over that unit, which is why a
backfill needs no special code path and can be parallelised by Airflow.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from pipeline.api.client import WeatherApiClient
from pipeline.catalogue import load_cities
from pipeline.config import Settings, get_settings
from pipeline.contracts import Contract, load_contract
from pipeline.landing import cleanup_temp_files, clear_partition, write_batch
from pipeline.logging_conf import get_logger
from pipeline.models import CONTRACT_VERSION, City, flatten_hourly, payload_hash
from pipeline.quality import check_batch
from pipeline.warehouse.connection import snowflake_connection
from pipeline.warehouse.loader import already_loaded, load_batch

log = get_logger(__name__)


@dataclass
class RunSummary:
    partitions_attempted: int = 0
    partitions_loaded: int = 0
    partitions_skipped: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_rejected: int = 0

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


def date_range(start: date, end: date) -> Iterator[date]:
    """Inclusive on both ends - the way humans mean it on the command line."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def max_available_date(today: date, contract: Contract) -> date:
    """Newest day the producer can be expected to have.

    Asking for anything newer returns an empty series, which would look exactly
    like data loss to the freshness tests downstream.
    """
    return today - timedelta(days=contract.source_lag_days)


def ingest_day(
    city: City,
    day: date,
    client: WeatherApiClient,
    contract: Contract,
    settings: Settings,
    run_id: str,
    connection: object | None = None,
    full_refresh: bool = False,
) -> tuple[int, int, int]:
    """Ingest one (city, day). Returns (inserted, updated, rejected)."""
    payload = client.fetch_hourly(city, day, day)
    source_hash = payload_hash(payload)

    if (
        connection is not None
        and not full_refresh
        and already_loaded(connection, city.city_id, day, source_hash)  # type: ignore[arg-type]
    ):
        log.info("run.skipped_unchanged", city_id=city.city_id, day=day.isoformat())
        return 0, 0, 0

    observations = flatten_hourly(city, payload)
    if not observations:
        # An empty window for a past day is a real anomaly, not an empty batch.
        raise ValueError(f"{city.city_id} {day}: producer returned no hourly rows")

    rows = [obs.model_dump() for obs in observations]
    accepted_rows, report = check_batch(
        contract=contract,
        rows=rows,
        city_id=city.city_id,
        observation_date=day,
        quarantine_root=Path(settings.landing_zone_root).parent / "quarantine",
    )
    accepted = [obs for obs in observations if obs.model_dump() in accepted_rows]

    if full_refresh:
        clear_partition(settings.landing_zone_root, day, city.city_id)

    batch = write_batch(
        root=settings.landing_zone_root,
        city_id=city.city_id,
        observation_date=day,
        observations=accepted,
        source_payload_hash=source_hash,
    )

    if connection is None:
        log.info("run.landed_only", city_id=city.city_id, day=day.isoformat(), rows=batch.row_count)
        return 0, 0, report.rejected_rows

    result = load_batch(
        connection=connection,  # type: ignore[arg-type]
        batch=batch,
        target=f"{settings.raw_fqn}.WEATHER_OBSERVATION",
        stage=f"{settings.raw_fqn}.STG_WEATHER_OBSERVATION",
        run_id=run_id,
        rows_fetched=len(rows),
        rows_rejected=report.rejected_rows,
    )
    return result.rows_inserted, result.rows_updated, report.rejected_rows


def run(
    start: date,
    end: date,
    run_id: str,
    city_ids: list[str] | None = None,
    settings: Settings | None = None,
    full_refresh: bool = False,
    land_only: bool = False,
) -> RunSummary:
    cfg = settings or get_settings()
    contract = load_contract("weather_observation", CONTRACT_VERSION, cfg.contracts_dir)
    cities = load_cities(only=city_ids)

    ceiling = max_available_date(date.today(), contract)
    if end > ceiling:
        log.warning(
            "run.window_clamped", requested_end=end.isoformat(), clamped_to=ceiling.isoformat()
        )
        end = ceiling
    if start > end:
        log.warning("run.nothing_to_do", start=start.isoformat(), end=end.isoformat())
        return RunSummary()

    cleanup_temp_files(cfg.landing_zone_root)
    summary = RunSummary()

    # nullcontext rather than a conditional expression: a ternary over two
    # unrelated context managers widens to `object`, and `with` on an `object`
    # is exactly the kind of thing the type checker is here to catch.
    connection_ctx: AbstractContextManager[Any]
    if land_only:  # noqa: SIM108 - a ternary here widens the type to `object`
        connection_ctx = nullcontext()
    else:
        connection_ctx = snowflake_connection(cfg, query_tag=run_id)
    with (
        connection_ctx as connection,
        WeatherApiClient(
            base_url=cfg.weather_api_base_url,
            timeout_seconds=cfg.weather_api_timeout_seconds,
            max_retries=cfg.weather_api_max_retries,
        ) as client,
    ):
        for day in date_range(start, end):
            for city in cities:
                summary.partitions_attempted += 1
                inserted, updated, rejected = ingest_day(
                    city=city,
                    day=day,
                    client=client,
                    contract=contract,
                    settings=cfg,
                    run_id=run_id,
                    connection=connection,
                    full_refresh=full_refresh,
                )
                if inserted or updated:
                    summary.partitions_loaded += 1
                else:
                    summary.partitions_skipped += 1
                summary.rows_inserted += inserted
                summary.rows_updated += updated
                summary.rows_rejected += rejected

    log.info("run.finished", run_id=run_id, **summary.as_dict())
    return summary
