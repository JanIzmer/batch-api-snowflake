# Cost model

Numbers below assume Snowflake Standard edition in `eu-central-1` at **$3 per
credit**. XS = 1 credit/hour, S = 2 credits/hour, billed **per second with a
60-second minimum** each time a warehouse resumes.

## What one daily run costs

Volume per run: 8 cities × 24 hours = **192 rows**, about 12 KB of Snappy
Parquet.

| Stage | Warehouse | Wall clock | Credits | USD |
|---|---|---|---|---|
| `PUT` × 8 + `MERGE` × 8 | `WH_INGEST_XS` | ~35 s work, 60 s minimum billing | 0.017 | $0.05 |
| Auto-suspend idle (60 s) | `WH_INGEST_XS` | 60 s | 0.017 | $0.05 |
| `dbt build` (2 staging views, 3 marts, ~30 tests) | `WH_TRANSFORM_S` | ~110 s | 0.061 | $0.18 |
| Auto-suspend idle (120 s) | `WH_TRANSFORM_S` | 120 s | 0.067 | $0.20 |
| **Total per run** | | ~6 min | **≈ 0.16** | **≈ $0.48** |

Roughly **$15/month**, of which about **half is idle time**, not query time.
That is the single most important fact about this pipeline's cost: at this
volume the work is free and the *warehouse being awake* is what is billed.

Storage is a rounding error: 192 rows/day × 365 = 70 k rows/year ≈ 4 MB
compressed, under $0.01/month. Cloud services stay inside the 10% free
allowance.

## Why the design looks the way it does

### Two warehouses, not one

`WH_INGEST_XS` (XS) and `WH_TRANSFORM_S` (S). Sharing one S warehouse would
make every tiny `MERGE` pay Small-sized credits, doubling the ingest line above
for no speedup — a 192-row MERGE does not go faster on more nodes. Splitting
also makes the bill legible: the two lines answer "is ingestion or modelling
getting expensive?" without any query analysis.

### Auto-suspend at 60 s / 120 s, not 600 s

The default 10-minute auto-suspend would bill ~0.17 credits of idle per resume
— ten times the actual work here. Short suspend costs a few seconds of resume
latency per task, which nothing in a batch pipeline cares about.

The two values differ on purpose: dbt runs many statements back to back with
short gaps between them, so a 60 s suspend would make it resume repeatedly
mid-build. 120 s covers the gaps.

### No `CLUSTER BY` on `RAW.WEATHER_OBSERVATION`

Automatic clustering is a background service that is billed separately and
continuously. It earns its keep on large tables with selective filters — the
usual rule of thumb is past a few hundred GB. This table is ~4 MB/year. Rows
also arrive in `observation_date` order, so Snowflake's automatic
micro-partitioning already correlates well with the way the table is queried.
Adding clustering here would be paying a maintenance service to reorganise data
that is already ordered.

### `cluster_by = ['observation_date']` on the marts

The opposite reasoning: `fct_weather_hourly` is what BI queries, always filtered
by date, and it is the table that grows if the city list does. Declaring the
clustering key now is free while the table is small and avoids a painful
reorganisation later. If the fact stays this small it will simply never trigger
reclustering.

### Partitioning of the landing zone by `observation_date` then `city_id`

That order matters, and it comes from the access pattern, not from aesthetics:

* every read and every reprocess is scoped to a **date** first (a failed night,
  a backfill range), and only sometimes to a city;
* dates are unbounded and cities are few, so date-first keeps directory fan-out
  balanced as history grows;
* the pair `(date, city)` is exactly the unit of work, so "delete and rebuild
  one partition" maps to `rm -rf` of one directory.

Partitioning city-first would mean a one-day backfill touching every city
directory, and a new city creating a new top-level tree.

### MERGE over DELETE + INSERT

A `DELETE WHERE observation_date = X` followed by `INSERT` rewrites every
micro-partition of that day even when nothing changed. The MERGE's
`WHEN MATCHED AND hash <> hash` guard means an unchanged re-run writes **zero**
micro-partitions. This is why re-running a week costs almost nothing.

### Restatement window of 7 days, not 30

Each extra day in the window is re-aggregated on every run. Seven days covers
the producer's observed restatement behaviour with margin. Thirty would
quadruple the mart's per-run scan for corrections that, in practice, never
arrive that late. It is a `var`, so a backfill overrides it for one run rather
than the setting being raised permanently.

## What a backfill costs

Full year, 8 cities: 2 920 `(city, day)` units.

| Stage | Credits | USD |
|---|---|---|
| 2 920 × (PUT + MERGE) on XS, ~1.5 s each | ~1.2 | $3.60 |
| One `dbt build` over 70 k rows on S | ~0.15 | $0.45 |
| **Total** | **≈ 1.4** | **≈ $4** |

A full year of history costs about four dollars and takes roughly 25 minutes,
limited by the 6-slot API pool rather than by Snowflake. This is why the
backfill DAG runs dbt **once at the end**: doing it per day would add 365 × 0.06
credits ≈ $65 of pure repetition.

## Where the cost would actually go if this grew

The design holds until roughly 10⁸ rows. The order in which things start to
hurt:

1. **Idle time stops dominating** once runs exceed a few minutes — then query
   tuning starts to matter and auto-suspend stops being the main lever.
2. **The restatement window becomes the largest scan.** At 10 k cities a 7-day
   window is 1.7 M rows per run; that is when it should shrink or become
   partition-pruned by `observation_date` explicitly.
3. **Automatic clustering on the fact becomes worth paying for** — around a few
   hundred GB, not before.
4. **The PUT-per-partition loop becomes the bottleneck**, not the MERGE. The fix
   is an external stage over the landing bucket and one `COPY` per day rather
   than one per city.

## How these numbers are measured

Every connection sets `QUERY_TAG` to the Airflow `dag_run_id`, so the cost of
any single run is attributable:

```sql
select
    query_tag,
    count(*)                                          as queries,
    sum(execution_time) / 1000                        as seconds,
    sum(credits_used_cloud_services)                  as cloud_services_credits
from snowflake.account_usage.query_history
where start_time >= dateadd('day', -7, current_timestamp())
  and query_tag ilike 'scheduled__%'
group by 1
order by seconds desc;
```

and warehouse-level truth, which is what is actually billed:

```sql
select
    warehouse_name,
    date_trunc('day', start_time) as day,
    sum(credits_used)             as credits,
    sum(credits_used) * 3         as usd
from snowflake.account_usage.warehouse_metering_history
where start_time >= dateadd('day', -30, current_timestamp())
group by 1, 2
order by 2 desc, 1;
```

A resource monitor caps the damage from a runaway backfill:

```sql
create resource monitor if not exists rm_weather
  with credit_quota = 50
  frequency = monthly
  start_timestamp = immediately
  triggers
    on 75 percent do notify
    on 90 percent do notify
    on 100 percent do suspend;

alter warehouse WH_INGEST_XS set resource_monitor = rm_weather;
alter warehouse WH_TRANSFORM_S set resource_monitor = rm_weather;
```

50 credits/month is ~3× the expected spend: high enough never to fire on a
normal month, low enough that a mistake is caught the same day instead of
appearing on an invoice.
