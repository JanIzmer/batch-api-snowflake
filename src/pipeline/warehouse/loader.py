"""Load a landed partition into Snowflake, idempotently.

The sequence for one (city, day):

    PUT file -> internal stage  (OVERWRITE=TRUE, path keyed by batch id)
    MERGE from stage -> RAW.WEATHER_OBSERVATION on the contract primary key
    REMOVE staged file
    INSERT one row into OPS.LOAD_AUDIT

Running it twice changes nothing the second time: the PUT overwrites the same
staged object, and the MERGE matches every row it inserted the first time and
finds the payload hash unchanged, so it updates nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from pipeline.landing import LandedBatch
from pipeline.logging_conf import get_logger
from pipeline.models import CONTRACT_VERSION
from pipeline.warehouse.connection import Connection

log = get_logger(__name__)

MERGE_TEMPLATE = (Path(__file__).parent / "ddl" / "003_merge.sql").read_text(encoding="utf-8")


@dataclass(frozen=True)
class LoadResult:
    batch_id: str
    city_id: str
    observation_date: date
    rows_inserted: int
    rows_updated: int
    status: str


def _stage_path(city_id: str, observation_date: date) -> str:
    return f"v{CONTRACT_VERSION}/{observation_date.isoformat()}/{city_id}"


def load_batch(
    connection: Connection,
    batch: LandedBatch,
    target: str,
    stage: str,
    run_id: str,
    rows_fetched: int,
    rows_rejected: int,
) -> LoadResult:
    started_at = datetime.now(tz=timezone.utc)
    cursor = connection.cursor()
    stage_path = _stage_path(batch.city_id, batch.observation_date)

    try:
        # AUTO_COMPRESS=FALSE: the parquet is already snappy compressed.
        cursor.execute(
            f"PUT 'file://{batch.path.resolve()}' '@{stage}/{stage_path}' "
            f"OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
        )

        merge_sql = MERGE_TEMPLATE.format(
            target=target, stage=stage, stage_path=f"{stage_path}/{batch.path.name}"
        )
        cursor.execute(merge_sql)
        inserted, updated = _merge_counts(cursor)

        # Staged files are not free and the MERGE has already consumed them.
        cursor.execute(f"REMOVE '@{stage}/{stage_path}/{batch.path.name}'")
        connection.commit()

        result = LoadResult(
            batch_id=batch.batch_id,
            city_id=batch.city_id,
            observation_date=batch.observation_date,
            rows_inserted=inserted,
            rows_updated=updated,
            status="loaded",
        )
    except Exception as exc:
        connection.rollback()
        write_audit(
            connection,
            run_id=run_id,
            batch=batch,
            rows_fetched=rows_fetched,
            rows_rejected=rows_rejected,
            rows_inserted=0,
            rows_updated=0,
            status="failed",
            error_message=str(exc)[:2000],
            started_at=started_at,
        )
        raise
    finally:
        cursor.close()

    write_audit(
        connection,
        run_id=run_id,
        batch=batch,
        rows_fetched=rows_fetched,
        rows_rejected=rows_rejected,
        rows_inserted=result.rows_inserted,
        rows_updated=result.rows_updated,
        status=result.status,
        error_message=None,
        started_at=started_at,
    )
    log.info(
        "snowflake.merged",
        city_id=batch.city_id,
        observation_date=batch.observation_date.isoformat(),
        inserted=result.rows_inserted,
        updated=result.rows_updated,
    )
    return result


def _merge_counts(cursor: object) -> tuple[int, int]:
    """Snowflake returns (rows_inserted, rows_updated) for a MERGE."""
    row = cursor.fetchone()  # type: ignore[attr-defined]
    if not row:
        return 0, 0
    values = list(row)
    inserted = int(values[0]) if len(values) > 0 and values[0] is not None else 0
    updated = int(values[1]) if len(values) > 1 and values[1] is not None else 0
    return inserted, updated


def write_audit(
    connection: Connection,
    run_id: str,
    batch: LandedBatch,
    rows_fetched: int,
    rows_rejected: int,
    rows_inserted: int,
    rows_updated: int,
    status: str,
    error_message: str | None,
    started_at: datetime,
) -> None:
    """One audit row per attempt, success or failure.

    Written on its own transaction so a failed load still leaves a trace.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO OPS.LOAD_AUDIT (
                RUN_ID, DATASET, CONTRACT_VERSION, CITY_ID, OBSERVATION_DATE, BATCH_ID,
                SOURCE_PAYLOAD_HASH, ROWS_FETCHED, ROWS_ACCEPTED, ROWS_REJECTED,
                ROWS_INSERTED, ROWS_UPDATED, STATUS, ERROR_MESSAGE,
                STARTED_AT_UTC, FINISHED_AT_UTC
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                "weather_observation",
                CONTRACT_VERSION,
                batch.city_id,
                batch.observation_date,
                batch.batch_id,
                batch.payload_hash,
                rows_fetched,
                batch.row_count,
                rows_rejected,
                rows_inserted,
                rows_updated,
                status,
                error_message,
                started_at.replace(tzinfo=None),
                datetime.now(tz=timezone.utc).replace(tzinfo=None),
            ),
        )
        connection.commit()
    finally:
        cursor.close()


def already_loaded(
    connection: Connection, city_id: str, observation_date: date, payload_hash: str
) -> bool:
    """True when this exact payload has already been loaded successfully.

    This is the cheap guard that makes a retried Airflow task a no-op instead of
    a second MERGE over identical data.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT 1
            FROM OPS.LOAD_AUDIT
            WHERE CITY_ID = %s
              AND OBSERVATION_DATE = %s
              AND SOURCE_PAYLOAD_HASH = %s
              AND STATUS = 'loaded'
            LIMIT 1
            """,
            (city_id, observation_date, payload_hash),
        )
        return cursor.fetchone() is not None
    finally:
        cursor.close()
