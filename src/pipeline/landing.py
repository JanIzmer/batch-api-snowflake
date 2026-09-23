"""Landing zone writer.

Raw-but-typed Parquet lands in a Hive style layout:

    <root>/weather_observation/v1/observation_date=YYYY-MM-DD/city_id=<id>/<batch_id>.parquet

Three properties matter:

1. **Deterministic paths.** The filename is the batch id, derived from
   (city_id, observation_date, contract_version). Re-running a day overwrites
   exactly the files that day produced instead of appending a second copy.
2. **Atomic publish.** Parquet is written to `.<name>.tmp` and renamed. A task
   killed mid-write leaves a temp file that the next run cleans up; a reader
   never sees a half written file.
3. **Partition = the unit of reprocessing.** One day of one city can be deleted
   and rebuilt without touching anything else, which is what backfill needs.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.logging_conf import get_logger
from pipeline.models import CONTRACT_VERSION, WeatherObservation, batch_id

log = get_logger(__name__)

DATASET = "weather_observation"

SCHEMA = pa.schema(
    [
        pa.field("city_id", pa.string(), nullable=False),
        pa.field("latitude", pa.float64(), nullable=False),
        pa.field("longitude", pa.float64(), nullable=False),
        pa.field("observed_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("temperature_2m_c", pa.float64()),
        pa.field("relative_humidity_2m_pct", pa.float64()),
        pa.field("precipitation_mm", pa.float64()),
        pa.field("wind_speed_10m_kmh", pa.float64()),
        pa.field("weather_code", pa.int32()),
        pa.field("_ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("_batch_id", pa.string(), nullable=False),
        pa.field("_source_payload_hash", pa.string(), nullable=False),
        pa.field("_contract_version", pa.int32(), nullable=False),
    ]
)


@dataclass(frozen=True)
class LandedBatch:
    path: Path
    batch_id: str
    city_id: str
    observation_date: date
    row_count: int
    payload_hash: str


def partition_dir(root: Path, observation_date: date, city_id: str) -> Path:
    return (
        Path(root)
        / DATASET
        / f"v{CONTRACT_VERSION}"
        / f"observation_date={observation_date.isoformat()}"
        / f"city_id={city_id}"
    )


def write_batch(
    root: Path | str,
    city_id: str,
    observation_date: date,
    observations: list[WeatherObservation],
    source_payload_hash: str,
) -> LandedBatch:
    """Write one (city, day) partition atomically and return its descriptor."""
    ingested_at = datetime.now(tz=UTC)
    bid = batch_id(city_id, observation_date)

    records: list[dict[str, Any]] = []
    for obs in observations:
        row = obs.model_dump()
        row.update(
            {
                "_ingested_at_utc": ingested_at,
                "_batch_id": bid,
                "_source_payload_hash": source_payload_hash,
                "_contract_version": CONTRACT_VERSION,
            }
        )
        records.append(row)

    target_dir = partition_dir(Path(root), observation_date, city_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    final_path = target_dir / f"{bid}.parquet"
    tmp_path = target_dir / f".{bid}.parquet.tmp"

    table = pa.Table.from_pylist(records, schema=SCHEMA)
    pq.write_table(table, tmp_path, compression="snappy")
    tmp_path.replace(final_path)

    log.info(
        "landing.written",
        city_id=city_id,
        observation_date=observation_date.isoformat(),
        rows=len(records),
        path=str(final_path),
    )
    return LandedBatch(
        path=final_path,
        batch_id=bid,
        city_id=city_id,
        observation_date=observation_date,
        row_count=len(records),
        payload_hash=source_payload_hash,
    )


def existing_payload_hash(root: Path | str, city_id: str, observation_date: date) -> str | None:
    """Hash of the payload already landed for this partition, if any.

    Used to short-circuit a re-run whose source data has not changed: we still
    want the run to succeed (so the DAG is green), we just skip the reload.
    """
    path = (
        partition_dir(Path(root), observation_date, city_id)
        / f"{batch_id(city_id, observation_date)}.parquet"
    )
    if not path.exists():
        return None
    try:
        table = pq.read_table(path, columns=["_source_payload_hash"])
    except (OSError, pa.ArrowInvalid):  # pragma: no cover - corrupt local file
        log.warning("landing.unreadable", path=str(path))
        return None
    if table.num_rows == 0:
        return None
    return str(table.column("_source_payload_hash")[0].as_py())


def clear_partition(root: Path | str, observation_date: date, city_id: str) -> None:
    """Remove a partition so it can be rebuilt from scratch (`--full-refresh`)."""
    target = partition_dir(Path(root), observation_date, city_id)
    if target.exists():
        shutil.rmtree(target)
        log.info("landing.cleared", path=str(target))


def cleanup_temp_files(root: Path | str) -> int:
    """Delete leftovers from tasks that were killed mid-write."""
    removed = 0
    for tmp in Path(root).rglob(".*.parquet.tmp"):
        tmp.unlink(missing_ok=True)
        removed += 1
    if removed:
        log.info("landing.temp_cleaned", files=removed)
    return removed
