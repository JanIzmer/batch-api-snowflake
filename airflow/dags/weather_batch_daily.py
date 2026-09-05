"""Daily weather batch DAG.

Shape
-----
    preflight -> ingest[city] (mapped) -> dbt seed -> dbt run -> dbt test -> report

Design decisions worth knowing before changing anything here:

* **The DAG owns the schedule, not the dates.** Every task derives its target
  day from `data_interval_start`, never from `date.today()`. A run for
  2026-08-01 fetches 2026-08-01 whether it executes on time or three weeks
  later, which is what makes `catchup=True` a correct backfill rather than a
  way to load the same day N times.
* **`catchup=True` with `max_active_runs=3`.** Catch-up is how a missed night
  repairs itself; the cap stops a two week outage from firing fourteen
  concurrent runs at a free API and an XS warehouse.
* **One mapped task per city.** A single city failing (bad coordinates, a
  producer gap) fails one task, and the retry re-runs only that city.
* **`depends_on_past=False`.** Days are independent, so a stuck 1 August must
  not block 2 August. The completeness test is what surfaces the hole instead.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from alerting import notify_on_retry, notify_on_sla_miss, page_on_dag_failure, page_on_failure

PROJECT_DIR = "/opt/pipeline"
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "SNOWFLAKE_ACCOUNT": "{{ var.value.snowflake_account }}",
    "SNOWFLAKE_USER": "{{ var.value.snowflake_user }}",
    "SNOWFLAKE_PASSWORD": "{{ var.value.snowflake_password }}",
    "SNOWFLAKE_DATABASE": "{{ var.value.get('snowflake_database', 'WEATHER') }}",
    "AIRFLOW_CTX_DAG_RUN_ID": "{{ run_id }}",
}

# The producer publishes with a two day lag, so the run on day D targets D-2.
SOURCE_LAG_DAYS = 2

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    # Exponential backoff: a transient API wobble clears in seconds, a real
    # outage takes minutes, and retrying every 30s in between just adds load.
    "retry_delay": pendulum.duration(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
    "on_failure_callback": page_on_failure,
    "on_retry_callback": notify_on_retry,
    "execution_timeout": pendulum.duration(minutes=45),
}

with DAG(
    dag_id="weather_batch_daily",
    description="Fetch a day of weather observations, land them and rebuild the marts",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=True,
    max_active_runs=3,
    default_args=default_args,
    on_failure_callback=page_on_dag_failure,
    sla_miss_callback=notify_on_sla_miss,
    tags=["weather", "batch", "snowflake", "dbt"],
    doc_md=__doc__,
    params={
        "cities": [],
        "full_refresh": False,
    },
) as dag:

    start = EmptyOperator(task_id="start")

    @task(task_id="target_date")
    def target_date(data_interval_start=None) -> str:
        """Resolve the day this run is responsible for.

        Returned as a string so it survives XCom serialisation unambiguously
        and shows up readably in the UI.
        """
        target = data_interval_start.subtract(days=SOURCE_LAG_DAYS).date()
        print(f"run targets observation_date={target}")
        return target.isoformat()

    @task(task_id="preflight")
    def preflight(day: str) -> list[str]:
        """Fail fast on the things that make the rest of the run pointless.

        Checks the contract parses and the city catalogue is readable, then
        returns the city ids to fan out over.
        """
        import sys

        sys.path.insert(0, f"{PROJECT_DIR}/src")
        from pipeline.catalogue import load_cities
        from pipeline.contracts import load_contract

        contract = load_contract("weather_observation", 1, f"{PROJECT_DIR}/contracts")
        cities = load_cities(f"{PROJECT_DIR}/seeds/city_catalogue.csv")
        print(f"contract {contract.name} v{contract.version}, {len(cities)} cities, day {day}")
        if not cities:
            raise ValueError("city catalogue is empty - nothing to ingest")
        return [city.city_id for city in cities]

    day = target_date()
    city_ids = preflight(day)

    with TaskGroup(group_id="ingest") as ingest_group:

        @task(task_id="ingest_city", max_active_tis_per_dag=4)
        def ingest_city(city_id: str, day: str, full_refresh: bool = False) -> dict:
            """Ingest one (city, day). Idempotent: safe to retry at any point."""
            import sys

            sys.path.insert(0, f"{PROJECT_DIR}/src")
            from airflow.operators.python import get_current_context

            from pipeline.config import get_settings
            from pipeline.run import run as run_pipeline

            context = get_current_context()
            summary = run_pipeline(
                start=pendulum.parse(day).date(),
                end=pendulum.parse(day).date(),
                run_id=context["run_id"],
                city_ids=[city_id],
                settings=get_settings(),
                full_refresh=full_refresh,
            )
            return summary.as_dict()

        ingested = ingest_city.partial(
            day=day, full_refresh="{{ params.full_refresh }}"
        ).expand(city_id=city_ids)

    # dbt runs once for the whole day, not once per city: the marts are
    # incremental over a restatement window, so per-city runs would rebuild the
    # same partitions eight times over.
    with TaskGroup(group_id="transform") as transform_group:

        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_DIR} && dbt deps --no-write-json",
            env=DBT_ENV,
            append_env=True,
        )

        dbt_seed = BashOperator(
            task_id="dbt_seed",
            bash_command=f"cd {DBT_DIR} && dbt seed --target prod",
            env=DBT_ENV,
            append_env=True,
        )

        dbt_source_freshness = BashOperator(
            task_id="dbt_source_freshness",
            # A stale source means the load silently did nothing; better to know
            # before spending warehouse credits transforming yesterday's data.
            bash_command=f"cd {DBT_DIR} && dbt source freshness --target prod",
            env=DBT_ENV,
            append_env=True,
        )

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"cd {DBT_DIR} && dbt build --target prod --select state:modified+ --defer --state ./state || dbt build --target prod",
            env=DBT_ENV,
            append_env=True,
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_DIR} && dbt test --target prod --store-failures",
            env=DBT_ENV,
            append_env=True,
            # Tests are the last gate before the dashboard reads the marts, so
            # this task carries the SLA for the whole run.
            sla=pendulum.duration(hours=1, minutes=30),
        )

        dbt_deps >> dbt_seed >> dbt_source_freshness >> dbt_run >> dbt_test

    @task(task_id="report")
    def report(summaries: list[dict], day: str) -> dict:
        """Roll the per-city summaries into one line for the run log."""
        totals = {
            "day": day,
            "cities": len(summaries),
            "rows_inserted": sum(s.get("rows_inserted", 0) for s in summaries),
            "rows_updated": sum(s.get("rows_updated", 0) for s in summaries),
            "rows_rejected": sum(s.get("rows_rejected", 0) for s in summaries),
            "partitions_skipped": sum(s.get("partitions_skipped", 0) for s in summaries),
        }
        print(f"run summary: {totals}")
        return totals

    finish = EmptyOperator(task_id="finish")

    start >> day >> city_ids >> ingest_group >> transform_group
    transform_group >> report(ingested, day) >> finish
