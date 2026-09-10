# Runbook

> The scenario this is written for: **the pipeline failed at 03:00.**
> How do you find out, how do you triage, how do you fix it, and how does the
> missed data get caught up.

## 1. How you find out

| Signal | Channel | Fires when |
|---|---|---|
| `on_failure_callback` → `page_on_failure` | page webhook | A task failed **after its last retry**. Never on an intermediate retry. |
| `on_failure_callback` on the DAG | page webhook | One message per failed run, so a fan-out failure is not eight pages. |
| `sla_miss_callback` | notify channel | `dbt_test` has not finished 1 h 30 m after the run started — i.e. the 07:00 dashboard refresh is at risk. |
| `dbt source freshness` | task failure → page | RAW has not been written in 30 h (warn) / 48 h (error). This is the backstop that catches a run which never started at all. |
| `assert_no_missing_days` | notify | A calendar hole exists. Catches failures that were missed entirely. |

Every alert payload carries `dag_id`, `task_id`, `run_id`, `logical_date`,
`try_number`, the Airflow `log_url` and a link back to this file.

**The most important property: the scheduler failing to run at all is also
detected.** An alert that only fires when a task fails cannot tell you about a
DAG that never started. Source freshness and the missing-days test both work
from the data, not from the orchestrator, so they fire either way.

## 2. Triage (target: 5 minutes)

Decide **is this urgent** before deciding what broke.

```
Did the load fail, or the transform?
├─ Load (ingest_city) failed
│  ├─ every city → producer-wide or credentials  → §3.1 / §3.4
│  └─ one city  → that city's data               → §3.2
└─ Transform (dbt) failed
   ├─ source freshness → the load silently did nothing → §3.3
   ├─ a test           → data is wrong, marts are built → §3.5
   └─ a model          → a code change → roll back the deploy
```

**Is it urgent?** The dashboard refreshes at 07:00 and the data is already two
days old by contract. A failure at 03:00 that is fixed by 06:00 is invisible to
consumers. So: if the fix is not obvious within ~20 minutes, note it, go back to
sleep, and fix it in the morning — *unless* `dbt_test` failed on a `unique` or
`not_null` test, which means wrong numbers are already queryable.

First query, always:

```sql
select run_id, city_id, observation_date, status, rows_fetched, rows_accepted,
       rows_rejected, rows_inserted, rows_updated, error_message, started_at_utc
from WEATHER.OPS.LOAD_AUDIT
where started_at_utc >= dateadd('hour', -12, current_timestamp())
order by started_at_utc desc;
```

This distinguishes the three cases that look identical in Airflow: never ran,
ran and failed, ran and loaded nothing.

## 3. Common causes and fixes

### 3.1 Producer outage (every city failed, transient errors in the log)

Check it is actually down:

```bash
curl -sS "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.4&start_date=2026-09-08&end_date=2026-09-08&hourly=temperature_2m" | head -c 300
```

* Still down → **do nothing tonight.** Clear the failed tasks in the morning;
  §4 catches up. Re-running now only fails again.
* Back up → clear the failed tasks and let Airflow retry:

```bash
airflow tasks clear weather_batch_daily \
  --task-regex 'ingest.*' --start-date 2026-09-08 --end-date 2026-09-08 --yes
```

### 3.2 One city failed

Read the reason in the audit table, then re-run that city alone:

```bash
docker compose run --rm cli \
  weather-pipeline ingest --date 2026-09-08 --city berlin
```

If it is a ragged-series or empty-window error, the producer's record for that
city is broken. Leave it; the completeness test will keep the gap visible, and
it usually resolves when the producer republishes.

### 3.3 Source freshness failed but no ingest task failed

The load ran and loaded nothing — the classic "green DAG, no data" case. Check
`LOAD_AUDIT.status`:

* `skipped_unchanged` everywhere → the payload hash matched, so the producer
  served identical data. If that is genuinely new data, the hash logic is
  wrong; force it: `weather-pipeline ingest --date <day> --full-refresh`.
* No rows at all → the DAG did not run. Check the scheduler is alive
  (`docker compose ps`, `airflow jobs check`).

### 3.4 Credentials expired

Every task fails at connection time. Rotate the Airflow Variable, then clear
the failed tasks. **Nothing needs re-fetching** — the Parquet is already in the
landing zone, so the retry only re-does the MERGE.

### 3.5 A dbt test failed

The marts are already built; a failed test does not un-build them. That is
deliberate: you need to be able to look at the bad data.

```sql
-- +store_failures: true materialises every failure into WEATHER.OPS
select * from WEATHER.OPS.unique_stg_weather__observations_observation_key limit 50;
```

| Test | Meaning | Action |
|---|---|---|
| `unique` / `unique_combination_of_columns` | Duplicated grain — a real correctness bug. | Find the duplicate `batch_id`s; a manual load with the wrong role is the usual cause. Urgent. |
| `relationships` on `city_id` | A fact references a city not in the seed. | Someone ingested a city without updating `seeds/city_catalogue.csv`. |
| `assert_no_future_observations` | A timestamp moved forward — double timezone application. | Urgent: it poisons "latest reading" queries. |
| `assert_daily_matches_hourly` | The marts were built from different snapshots. | `dbt build --select fct_weather_hourly+`. |
| `accepted_range` (warn) | Implausible value. | Not urgent. Quarantine review. |

## 4. Catching up

The whole point of `catchup=True` plus an idempotent `(city, day)` unit is that
catching up is not a special procedure.

**One or two missed days** — clear the runs; the scheduler re-runs them, capped
at 3 concurrent:

```bash
airflow dags backfill weather_batch_daily \
  --start-date 2026-09-07 --end-date 2026-09-08 --reset-dagruns --yes
```

**A longer outage** — use the `weather_backfill` DAG instead. It runs one mapped
task per `(city, day)` through the 6-slot `weather_api` pool and does **one**
dbt build at the end rather than one per day:

```json
{"start_date": "2026-08-20", "end_date": "2026-09-08", "cities": [], "full_refresh": false}
```

**Verify the catch-up actually worked** — do not trust green tasks, check the
data:

```sql
select observation_date, count(distinct city_id) as cities, sum(hours_observed) as hours
from WEATHER.MARTS.AGG_WEATHER_DAILY
where observation_date between '2026-08-20' and '2026-09-08'
group by 1 having count(distinct city_id) < 8 or sum(hours_observed) < 8 * 24
order by 1;
```

An empty result means the range is complete. Then re-run the completeness tests:

```bash
dbt test --select tag:completeness
```

## 5. Escalation and after

* Producer-side and lasting more than 24 h → tell the dashboard owners; the
  freshness banner is driven by `agg_weather_daily.last_ingested_at_utc`.
* Data already consumed and wrong → fix forward (MERGE corrects in place), then
  say in the channel which `observation_date` range changed and when.

Write a short note in the incident channel for anything that needed a manual
step: what fired, what the cause was, what you ran. If the same fix is needed
twice, it belongs in this file — and if it is needed a third time, it belongs in
the code.

## Quick reference

```bash
# What happened last night
docker compose logs airflow-scheduler --since 12h | grep -i error

# Re-run one partition
docker compose run --rm cli weather-pipeline ingest --date 2026-09-08 --city berlin

# Re-run a day for every city, ignoring the unchanged-payload shortcut
docker compose run --rm cli weather-pipeline ingest --date 2026-09-08 --full-refresh

# Rebuild the marts only
docker compose run --rm cli bash -lc "cd /opt/pipeline/dbt && dbt build --select marts"

# Which run cost what
# -> docs/cost_model.md, "How these numbers are measured"
```
