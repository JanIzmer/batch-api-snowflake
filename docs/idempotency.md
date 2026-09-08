# Idempotency and backfill

## The claim

Running the pipeline twice for the same day produces the same warehouse state as
running it once. Running it for a day that was already loaded, while the daily
DAG is also running, is safe. Neither creates duplicate rows.

## How it is achieved, layer by layer

### 1. The unit of work is `(city, day)`

Not "everything since last time". Every task derives its target day from the
Airflow `data_interval_start`, never from `date.today()`. A run for 2026-08-01
fetches 2026-08-01 whether it executes on schedule or three weeks later.

This is what makes `catchup=True` a correct backfill mechanism instead of a way
to load today's data fourteen times.

### 2. Deterministic batch identity

```
batch_id = sha256(city_id | observation_date | contract_version)[:32]
```

The landing file is named after it:

```
data/landing/weather_observation/v1/observation_date=2026-08-20/city_id=berlin/<batch_id>.parquet
```

A second run of that day writes to the same path. There is no timestamp in the
filename, so there is no way to accumulate a second copy of the same batch.
Writes go to `.<batch_id>.parquet.tmp` and are renamed, so a task killed
mid-write leaves a temp file (cleaned up on the next run) rather than a
truncated Parquet file that a reader would happily treat as complete.

### 3. MERGE, not COPY

`COPY INTO` is append-only: re-running a day would double its rows. The loader
instead stages the file and runs a `MERGE` on the contract's primary key:

```sql
ON  tgt.CITY_ID = src.city_id
AND tgt.OBSERVED_AT_UTC = src.observed_at_utc
WHEN MATCHED AND tgt._SOURCE_PAYLOAD_HASH <> src._source_payload_hash THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```

Three outcomes fall out of that one statement:

| Situation | Result |
|---|---|
| First load of a day | 24 inserts. |
| Re-run, producer data unchanged | 0 inserts, 0 updates. The payload hash matches, so the `WHEN MATCHED` branch does not fire and Snowflake writes no micro-partitions. |
| Re-run, producer restated history | 0 inserts, N updates. The corrected values overwrite in place. |

### 4. A cheaper guard in front of it

Before fetching a partition, the loader asks `OPS.LOAD_AUDIT` whether this exact
`(city, day, payload_hash)` has already been loaded successfully. If it has, the
whole partition is skipped — no PUT, no MERGE, no warehouse time. A retried
Airflow task therefore costs almost nothing.

### 5. Idempotency downstream too

The dbt marts are `incremental` with `incremental_strategy = 'merge'` and a
`unique_key`. They rebuild a **restatement window** (default 7 days) rather than
filtering on `> max(date)`:

```sql
{% raw %}{% if is_incremental() %}
where {{ restatement_filter('observation_date') }}
{% endif %}{% endraw %}
```

A `> max(date)` filter is the classic incremental bug: the first time a day is
loaded it is often incomplete, and a strict filter then freezes that incomplete
value forever. Re-processing the last week is cheap (a week of eight cities is
~1 300 rows) and means a late correction actually lands.

## Backfill

### Small range, or "just redo yesterday"

```bash
make ingest DATE=2026-08-20
# or
docker compose run --rm cli weather-pipeline ingest --start 2026-08-01 --end 2026-08-07
```

### A range, properly

Trigger the `weather_backfill` DAG with a config:

```json
{
  "start_date": "2026-07-01",
  "end_date": "2026-07-31",
  "cities": [],
  "full_refresh": false
}
```

It expands the range into one mapped task per `(city, day)` — so the UI shows
exactly how many units will run and a single failure retries alone — then runs
**one** dbt build at the end with the restatement window widened to cover the
whole range. Rebuilding the marts after every single day would repeat the same
work `N` times for no benefit.

Concurrency is capped by the Airflow pool `weather_api` (6 slots), shared with
the daily DAG. However many backfill units are queued, the producer never sees
more than six concurrent requests from us.

The DAG refuses ranges longer than 400 days. A fat-fingered `start_date` should
not start a decade-long run.

### When the data itself is wrong, not just missing

`--full-refresh` clears the landing partition before rewriting it. Use it when
the landed Parquet is suspect (a bug in flattening, a bad contract version).
For normal "the producer corrected the numbers" cases it is unnecessary — the
MERGE handles that on its own.

### Verifying a backfill did what you think

```sql
select observation_date, count(*) as cities, sum(rows_inserted), sum(rows_updated)
from WEATHER.OPS.LOAD_AUDIT
where run_id = '<dag_run_id>'
group by 1 order by 1;
```

A backfill over unchanged data reporting `partitions_skipped = 217` is
idempotency working, not a backfill that failed to do anything.
