"""Command line entry point.

Airflow calls exactly this CLI, so anything reproducible in a terminal is
reproducible in the DAG - including a backfill.

    weather-pipeline ingest --start 2026-08-01 --end 2026-08-31
    weather-pipeline ingest --date 2026-08-20 --city berlin --full-refresh
    weather-pipeline apply-ddl
"""

from __future__ import annotations

import sys
import uuid
from datetime import date, timedelta

import typer

from pipeline.config import get_settings
from pipeline.logging_conf import configure_logging, get_logger
from pipeline.quality import DataQualityError

app = typer.Typer(add_completion=False, help="Weather batch pipeline")
log = get_logger(__name__)


@app.callback()
def _configure(
    log_level: str = typer.Option("INFO", envvar="LOG_LEVEL"),
    json_logs: bool = typer.Option(False, "--json-logs", envvar="JSON_LOGS"),
) -> None:
    configure_logging(level=log_level, json_logs=json_logs)


@app.command()
def ingest(
    start: str = typer.Option(None, "--start", help="First day, inclusive (YYYY-MM-DD)."),
    end: str = typer.Option(None, "--end", help="Last day, inclusive (YYYY-MM-DD)."),
    day: str = typer.Option(None, "--date", help="Shorthand for --start X --end X."),
    city: list[str] = typer.Option(None, "--city", help="Restrict to these city ids."),
    full_refresh: bool = typer.Option(
        False, "--full-refresh", help="Drop and rebuild the landing partitions first."
    ),
    land_only: bool = typer.Option(
        False, "--land-only", help="Write parquet but do not touch Snowflake."
    ),
    run_id: str = typer.Option(None, "--run-id", envvar="AIRFLOW_CTX_DAG_RUN_ID"),
) -> None:
    """Ingest an inclusive date range. Defaults to the newest available day."""
    from pipeline.run import run as run_pipeline

    settings = get_settings()

    if day:
        start_date = end_date = date.fromisoformat(day)
    elif start:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end) if end else start_date
    else:
        start_date = end_date = date.today() - timedelta(days=settings.source_lag_days)

    resolved_run_id = run_id or f"manual__{uuid.uuid4().hex[:12]}"
    log.info(
        "cli.ingest",
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        cities=list(city) or "all",
        run_id=resolved_run_id,
        full_refresh=full_refresh,
    )

    try:
        summary = run_pipeline(
            start=start_date,
            end=end_date,
            run_id=resolved_run_id,
            city_ids=list(city) or None,
            settings=settings,
            full_refresh=full_refresh,
            land_only=land_only,
        )
    except DataQualityError as exc:
        # Exit code 2 is wired to a different Airflow alert than a crash: a
        # quality failure needs a data owner, a crash needs an engineer.
        log.error("cli.quality_failed", error=str(exc))
        raise typer.Exit(code=2) from exc

    typer.echo(
        f"partitions={summary.partitions_attempted} loaded={summary.partitions_loaded} "
        f"skipped={summary.partitions_skipped} inserted={summary.rows_inserted} "
        f"updated={summary.rows_updated} rejected={summary.rows_rejected}"
    )


@app.command("apply-ddl")
def apply_ddl_command() -> None:
    """Create databases, schemas, stages and tables. Safe to re-run."""
    from pipeline.warehouse.connection import apply_ddl, snowflake_connection

    with snowflake_connection(get_settings(), query_tag="apply-ddl") as connection:
        applied = apply_ddl(connection)
    typer.echo(f"applied {applied} statements")


@app.command("check-contract")
def check_contract(
    name: str = typer.Argument("weather_observation"),
    version: int = typer.Argument(1),
) -> None:
    """Parse a contract file and print its shape. Used as a CI smoke test."""
    from pipeline.contracts import load_contract

    contract = load_contract(name, version, get_settings().contracts_dir)
    typer.echo(f"{contract.name} v{contract.version}")
    typer.echo(f"  grain       : {contract.grain}")
    typer.echo(f"  primary key : {', '.join(contract.primary_key)}")
    typer.echo(f"  fields      : {len(contract.fields)}")


def main() -> None:  # pragma: no cover - thin wrapper
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":  # pragma: no cover
    main()
