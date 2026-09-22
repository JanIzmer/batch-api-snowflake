"""Manual backfill DAG.

`airflow dags backfill` re-runs the scheduled DAG, which is correct but slow:
it walks day by day and runs the full dbt build after each one. For a long
range that is a lot of warehouse time spent rebuilding the same partitions.

This DAG does the same work in the right shape for catching up:

    plan -> ingest[(city, day)] (mapped, pooled) -> dbt build once at the end

It is `schedule=None` and triggered with a config:

    {"start_date": "2026-07-01", "end_date": "2026-07-31",
     "cities": ["berlin", "warsaw"], "full_refresh": false}

Safe to run while the daily DAG is running: both go through the same idempotent
(city, day) unit, and the `weather_api` pool caps how hard we hit the producer.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from alerting import notify_on_retry, page_on_failure

PROJECT_DIR = "/opt/pipeline"
DBT_DIR = f"{PROJECT_DIR}/dbt"

# Bounded so a fat-fingered range cannot start a year long run by accident.
MAX_BACKFILL_DAYS = 400

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": page_on_failure,
    "on_retry_callback": notify_on_retry,
}

with DAG(
    dag_id="weather_backfill",
    description="Re-ingest an arbitrary date range, then rebuild the marts once",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "backfill", "manual"],
    doc_md=__doc__,
    params={
        "start_date": Param("2026-08-01", type="string", format="date"),
        "end_date": Param("2026-08-07", type="string", format="date"),
        "cities": Param([], type="array", description="Empty means every city."),
        "full_refresh": Param(False, type="boolean"),
    },
) as dag:

    @task(task_id="plan")
    def plan(params: dict | None = None) -> list[dict]:
        """Expand the requested range into one work item per (city, day).

        Doing the expansion here rather than inside the ingest task means the
        Airflow UI shows exactly how many units the backfill will run, and a
        single failed unit is retried on its own.
        """
        import sys

        sys.path.insert(0, f"{PROJECT_DIR}/src")
        from pipeline.catalogue import load_cities

        params = params or {}
        start = pendulum.parse(params["start_date"]).date()
        end = pendulum.parse(params["end_date"]).date()
        if end < start:
            raise ValueError(f"end_date {end} is before start_date {start}")

        span = (end - start).days + 1
        if span > MAX_BACKFILL_DAYS:
            raise ValueError(
                f"refusing to backfill {span} days in one run "
                f"(limit {MAX_BACKFILL_DAYS}); split it into several triggers"
            )

        cities = load_cities(
            f"{PROJECT_DIR}/seeds/city_catalogue.csv", only=params.get("cities") or None
        )
        work = [
            {"city_id": city.city_id, "day": start.add(days=offset).isoformat()}
            for offset in range(span)
            for city in cities
        ]
        print(f"backfill plan: {span} days x {len(cities)} cities = {len(work)} units")
        return work

    @task(
        task_id="ingest_unit",
        # A shared pool with the daily DAG: whichever runs, the producer sees
        # at most this many concurrent requests from us.
        pool="weather_api",
        max_active_tis_per_dag=6,
    )
    def ingest_unit(unit: dict, params: dict | None = None) -> dict:
        import sys

        sys.path.insert(0, f"{PROJECT_DIR}/src")
        from airflow.operators.python import get_current_context

        from pipeline.config import get_settings
        from pipeline.run import run as run_pipeline

        context = get_current_context()
        day = pendulum.parse(unit["day"]).date()
        summary = run_pipeline(
            start=day,
            end=day,
            run_id=context["run_id"],
            city_ids=[unit["city_id"]],
            settings=get_settings(),
            full_refresh=bool((params or {}).get("full_refresh", False)),
        )
        return summary.as_dict()

    @task(task_id="restatement_window_days")
    def restatement_window_days(params: dict | None = None) -> int:
        """How wide the mart rebuild has to be for this backfill.

        Computed in Python rather than in the Bash template. Date arithmetic in
        Jinja needs filters Airflow does not ship (`as_datetime` is a dbt
        filter, not an Airflow one), and a template that fails to render only
        fails at execution - here, after every ingest task has already run.
        """
        params = params or {}
        start = pendulum.parse(params["start_date"]).date()
        end = pendulum.parse(params["end_date"]).date()
        # Two days of slack so the boundary days are rebuilt as well.
        return (end - start).days + 2

    # One dbt build for the whole range. The marts merge on their grain, so
    # rebuilding a wide window at the end is both cheaper and more correct than
    # rebuilding after every day.
    rebuild_marts = BashOperator(
        task_id="rebuild_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt build --target prod "
            "--vars '{\"restatement_window_days\": "
            "{{ ti.xcom_pull(task_ids='restatement_window_days') }}}'"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_DIR,
            "AIRFLOW_CTX_DAG_RUN_ID": "{{ run_id }}",
        },
        append_env=True,
        execution_timeout=pendulum.duration(hours=2),
    )

    @task(task_id="verify")
    def verify(summaries: list[dict], params: dict | None = None) -> dict:
        """Report what the backfill actually changed.

        `partitions_skipped` being high is the expected outcome when a range is
        re-run against unchanged source data - that is idempotency working, not
        a backfill that did nothing.
        """
        totals = {
            "units": len(summaries),
            "rows_inserted": sum(s.get("rows_inserted", 0) for s in summaries),
            "rows_updated": sum(s.get("rows_updated", 0) for s in summaries),
            "partitions_skipped": sum(s.get("partitions_skipped", 0) for s in summaries),
            "rows_rejected": sum(s.get("rows_rejected", 0) for s in summaries),
        }
        print(f"backfill summary: {totals}")
        return totals

    work_items = plan()
    ingested = ingest_unit.expand(unit=work_items)
    window = restatement_window_days()

    ingested >> rebuild_marts
    window >> rebuild_marts
    rebuild_marts >> verify(ingested)
