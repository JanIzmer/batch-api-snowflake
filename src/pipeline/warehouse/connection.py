"""Snowflake connection handling.

Kept separate from the loader so tests can inject a fake connection and so the
session defaults (role, warehouse, query tag) live in exactly one place.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol

from pipeline.config import Settings, get_settings
from pipeline.logging_conf import get_logger

log = get_logger(__name__)

DDL_DIR = Path(__file__).parent / "ddl"


class Cursor(Protocol):
    """The slice of the DB-API cursor this project uses.

    Every parameter is positional-only. A protocol that names its parameters
    also requires the implementation to use the same names, and the real driver
    calls the first one `command` rather than `statement` - which made a
    perfectly good SnowflakeCursor fail to satisfy this protocol.
    """

    @property
    def rowcount(self) -> int | None: ...

    def execute(self, statement: str, params: Any = None, /) -> Any: ...
    def fetchone(self) -> Any: ...
    def fetchall(self) -> list[Any]: ...
    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self, /) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


@contextlib.contextmanager
def snowflake_connection(
    settings: Settings | None = None, query_tag: str | None = None
) -> Iterator[Connection]:
    """Open a connection with session defaults applied.

    `query_tag` is set to the Airflow run id by the caller. Every credit spent
    by this pipeline is then attributable in SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY,
    which is how the cost numbers in docs/cost_model.md are measured.
    """
    import snowflake.connector  # imported lazily: tests never need the driver

    cfg = settings or get_settings()
    connection = snowflake.connector.connect(
        account=cfg.snowflake_account,
        user=cfg.snowflake_user,
        password=cfg.snowflake_password.get_secret_value(),
        role=cfg.snowflake_role,
        warehouse=cfg.snowflake_warehouse,
        database=cfg.snowflake_database,
        schema=cfg.snowflake_raw_schema,
        client_session_keep_alive=False,
        # A runaway statement should die rather than hold a warehouse open.
        session_parameters={
            "STATEMENT_TIMEOUT_IN_SECONDS": 1800,
            "QUERY_TAG": query_tag or "batch-api-snowflake",
            "TIMEZONE": "UTC",
        },
    )
    log.info("snowflake.connected", account=cfg.snowflake_account, role=cfg.snowflake_role)
    try:
        yield connection
    finally:
        connection.close()
        log.info("snowflake.closed")


def split_statements(script: str) -> list[str]:
    """Split a .sql file into executable statements.

    Snowflake's connector executes one statement per call unless multi-statement
    is enabled, which needs an extra session parameter - splitting is simpler
    and keeps the error message pointing at the statement that actually failed.
    """
    statements = []
    for chunk in script.split(";"):
        cleaned = "\n".join(
            line for line in chunk.splitlines() if not line.strip().startswith("--")
        ).strip()
        if cleaned:
            statements.append(cleaned)
    return statements


def apply_ddl(connection: Connection, ddl_dir: Path = DDL_DIR) -> int:
    """Run every numbered DDL file in order. All statements are CREATE IF NOT
    EXISTS, so this is safe on every deploy."""
    applied = 0
    cursor = connection.cursor()
    try:
        for path in sorted(ddl_dir.glob("[0-9][0-9][0-9]_*.sql")):
            if path.name.endswith("_merge.sql") or "merge" in path.stem:
                continue  # templated, not a migration
            for statement in split_statements(path.read_text(encoding="utf-8")):
                cursor.execute(statement)
                applied += 1
            log.info("snowflake.ddl_applied", file=path.name)
    finally:
        cursor.close()
    connection.commit()
    return applied
