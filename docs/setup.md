# Setup

What has to exist before this runs, in the order it has to exist.

## What needs nothing from you

These work on a clean checkout with only Python and Docker:

| Check | Command | Needs |
|---|---|---|
| Unit tests (46) | `make install && make test` | Python 3.11+ |
| dbt project parses | `cd dbt && dbt deps && dbt parse --target dev` | dbt, dummy env vars |
| DAGs import, graph and templates | `make validate-dags` | `apache-airflow==2.10.2` |
| Ingestion without a warehouse | `weather-pipeline ingest --date <day> --land-only` | network only |
| CI | push | **no secrets** — CI parses and validates, it never connects |

`--land-only` is the useful one: the whole fetch → validate → quarantine →
Parquet path runs and writes to `data/landing/`, so the pipeline can be
demonstrated end to end without a Snowflake account existing at all.

## What needs a Snowflake account

A trial (30 days, ~$400 of credits) is more than enough — the whole project
costs about $15/month at full tilt, see [cost_model.md](cost_model.md).

### 1. Create the objects

Run as a role that can create warehouses and databases (`SYSADMIN` plus
`SECURITYADMIN` for the roles), in a Snowflake worksheet:

```sql
-- src/pipeline/warehouse/ddl/001_warehouses_and_roles.sql
-- src/pipeline/warehouse/ddl/002_raw_objects.sql
```

Or, once credentials are in `.env`, let the CLI do the second file:

```bash
make apply-ddl
```

### 2. Create the pipeline user

```sql
CREATE USER IF NOT EXISTS etl_loader
  PASSWORD = '<generate one>'
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = WH_INGEST_XS
  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE LOADER TO USER etl_loader;
GRANT ROLE TRANSFORMER TO USER etl_loader;
```

Key-pair auth is better than a password for anything real; the connector
supports it and `connection.py` is the only file that would change.

### 3. Fill `.env`

```bash
cp .env.example .env
```

`SNOWFLAKE_ACCOUNT` is the account identifier (`xy12345.eu-central-1`), not the
login URL.

### 4. Verify, in this order

```bash
make apply-ddl                      # objects exist
make ingest DATE=2026-09-08         # one day lands and MERGEs
make ingest DATE=2026-09-08         # run it AGAIN - must report 0 inserted, 0 updated
make dbt-build                      # seed + staging + marts + 40 tests
```

The second `make ingest` is the point of the whole project: if it reports
anything other than zeros, idempotency is broken.

## What Airflow needs

The DAGs read four Airflow Variables. Set them in the UI (Admin → Variables) or:

```bash
docker compose exec airflow-scheduler airflow variables set snowflake_account  '<account>'
docker compose exec airflow-scheduler airflow variables set snowflake_user     'etl_loader'
docker compose exec airflow-scheduler airflow variables set snowflake_password '<password>'
docker compose exec airflow-scheduler airflow variables set snowflake_database 'WEATHER'
```

A missing Variable fails the task at **render** time with an unhelpful message,
so set all four before unpausing anything.

The `weather_api` pool (6 slots) is created by `airflow-init` on first boot. If
the backfill DAG ever reports a missing pool, recreate it:

```bash
docker compose exec airflow-scheduler airflow pools set weather_api 6 'API concurrency cap'
```

## Optional

| Variable | Effect if unset |
|---|---|
| `ALERT_PAGE_WEBHOOK_URL` | alerts print to the task log instead of paging |
| `ALERT_NOTIFY_WEBHOOK_URL` | same, for the notify channel |

Both are read from the environment by `airflow/dags/alerting.py`, and a missing
webhook never turns a warning into a task failure.

## Known first-run friction

* **`dbt deps` needs network access** to hub.getdbt.com. `package-lock.yml` is
  committed, so versions are pinned, but the download still happens.
* **Airflow's first boot takes a minute or two** — `airflow-init` migrates the
  metadata database before the scheduler and webserver start. `make up` returns
  before the UI is ready.
* **The API has no auth but does rate limit.** The `weather_api` pool caps us
  at 6 concurrent requests; do not raise it to speed up a backfill.
