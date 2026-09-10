# Failure modes

Every row is a way this pipeline is known to break, how it is detected, what
happens automatically, and what a human has to do. The short version lives in
the README; this is the long one.

## Ingestion

### The API is down or times out

* **Detection:** `TransientApiError`, task retries visible in Airflow.
* **Automatic:** 5 retries inside the HTTP client with exponential backoff and
  jitter, then 3 Airflow task retries with exponential backoff up to 30 min.
  Total tolerated outage ≈ 90 minutes.
* **Blast radius:** one `(city, day)` task. Other cities continue.
* **Manual:** nothing, unless the outage outlives the retries — then the
  catch-up procedure in the [runbook](runbook.md) re-runs the missed day.
* **Why it is built this way:** jitter is not decoration. Eight mapped tasks
  retrying in lockstep is a self-inflicted DDoS on a free API, and the third
  retry would fail for a reason we caused.

### The API returns 4xx

* **Detection:** `PermanentApiError`, task fails on the **first** attempt.
* **Automatic:** no retry. Page.
* **Blast radius:** usually every city — a 4xx normally means a parameter we
  send is no longer valid.
* **Manual:** read the response body in the task log; it is a request we built,
  so the fix is in our code or in the catalogue.
* **Why:** retrying our own bad request just burns rate limit and delays the
  alert by an hour.

### The API returns 200 with a non-JSON body

* **Detection:** `TransientApiError("returned 200 with a non-JSON body")`.
* **Automatic:** retried.
* **Why:** in practice this is a proxy or captive portal, not the producer
  changing format. Treating it as transient is right far more often than not.

### Ragged series (one variable shorter than `time`)

* **Detection:** `ValueError` in `flatten_hourly`, task fails.
* **Automatic:** none; the batch is refused.
* **Blast radius:** one `(city, day)`.
* **Why it fails loudly:** if we zipped the arrays anyway, every value after
  the gap would be attached to the wrong hour. The row count would look
  perfect. A hard failure is much cheaper than silently shifted data.

### The producer returns an empty window for a past day

* **Detection:** `ValueError("producer returned no hourly rows")`.
* **Automatic:** task fails.
* **Why:** an empty result for a day that is two days old is an anomaly, not an
  empty batch. Landing zero rows would make the day "successfully loaded" and
  invisible to every completeness check.

### We ask for a day the producer has not published yet

* **Detection:** `run.window_clamped` in the logs.
* **Automatic:** the window is clamped to `today - source_lag_days` from the
  contract. No failure.
* **Why:** this is the normal state of a manual `--end` typed by a human.

## Data quality

### A handful of rows violate the contract

* **Detection:** `quality.quarantined` log line, `ROWS_REJECTED > 0` in
  `OPS.LOAD_AUDIT`.
* **Automatic:** rejected rows are written to
  `data/quarantine/<contract>/<version>/<date>/<city>.jsonl` with the reason
  per row; the rest of the batch loads.
* **Manual:** review the quarantine file the next working day.
* **Why not fail:** one bad hour should not block a whole day of history. The
  quarantine file, not a stack trace, is what tells you *which* rule broke.

### More than 2% of rows violate the contract

* **Detection:** `DataQualityError`, CLI exits with code 2, task fails.
* **Automatic:** nothing is loaded. Page, but tagged as a data issue.
* **Manual:** compare the payload against the contract; this is nearly always
  the producer having changed something.
* **Why a ratio, not a count:** one bad row in 24 is a sensor glitch, the same
  count in a 2 000-row backfill batch is noise. The threshold has to scale with
  the batch.

### An unannounced new field appears

* **Detection:** `quality.unknown_fields` warning.
* **Automatic:** field is ignored, batch loads.
* **Manual:** see the [contract decision table](data_contract.md).

### A unit or meaning changes silently (°C → °F)

* **Detection:** **weak.** Range tests at `warn`, and the reconciliation test,
  will not catch a change that stays inside plausible bounds.
* **Manual:** this is the failure mode this design is worst at, and pretending
  otherwise would be dishonest. Mitigations: subscribe to the producer's
  changelog, and treat a sudden shift in `avg_temperature_c` distribution as a
  signal rather than weather.

## Loading

### The load fails half way through a MERGE

* **Automatic:** rollback, an audit row with `STATUS = 'failed'` and the error
  message, then the exception propagates to Airflow.
* **Recovery:** retry. The MERGE is idempotent, the staged file was written
  with `OVERWRITE = TRUE`, so the retry starts from a clean state.
* **Why the audit row is written on a separate transaction:** a failed load that
  leaves no trace is the worst possible artefact at 03:00.

### A task is killed mid-write to the landing zone

* **Automatic:** the temp file `.<batch_id>.parquet.tmp` is left behind and
  deleted by `cleanup_temp_files` at the start of the next run.
* **Why:** Parquet written directly to its final name and truncated is still a
  readable-looking file to some readers. Write-then-rename makes publication
  atomic.

### Two runs load the same partition concurrently

* **Automatic:** both MERGE on the same key. The second finds the payload hash
  unchanged and updates nothing.
* **Why it is safe:** this is the daily DAG and a backfill overlapping, which is
  a normal thing to want to do during catch-up.

### Snowflake credentials expired

* **Detection:** connection error on every task.
* **Automatic:** retries, then page.
* **Manual:** rotate the Airflow Variable. Nothing needs re-landing — the
  Parquet is already on disk, so the re-run only re-does the MERGE.

## Transformation

### `dbt source freshness` fails

* **Detection:** the `dbt_source_freshness` task, before `dbt run`.
* **Automatic:** the DAG stops before spending transform credits.
* **Why it runs before `run` and not after:** if the source is stale the load
  silently did nothing, and transforming yesterday's data into today's marts is
  both wasted money and actively misleading.

### A `not_null` / `unique` test fails

* **Detection:** `dbt_test` task fails; failing rows are materialised into
  `WEATHER.OPS` because `+store_failures: true`.
* **Automatic:** page. The marts are already built — this is a gate on trust,
  not on materialisation.
* **Manual:** `select * from WEATHER.OPS.<test_name>` shows exactly which rows.

### A range test warns

* **Detection:** warning in the dbt output, run stays green.
* **Automatic:** notify channel only.
* **Why:** an impossible temperature reading is a conversation with the
  producer, not a reason to stop the dashboard from refreshing.

### Completeness: a day has fewer than 24 hours

* **Detection:** `assert_hourly_completeness` (severity `warn`).
* **Automatic:** notify. The day is still published but
  `agg_weather_daily.is_complete_day = false`.
* **Why warn:** the usual cause is the producer having published a partial day,
  which the next run's restatement window fixes by itself. Paging for something
  that self-heals is how alert channels get muted.

### Completeness: a day is missing entirely

* **Detection:** `assert_no_missing_days`.
* **Automatic:** notify.
* **Manual:** run the backfill DAG for the hole. This is the test that catches
  "a run failed three weeks ago and nobody noticed".

### The two marts disagree

* **Detection:** `assert_daily_matches_hourly` (severity `error`).
* **Cause:** almost always a mart refreshed while its upstream was mid-load.
* **Manual:** `dbt build --select fct_weather_hourly+` to rebuild in order.

## Orchestration

### The DAG does not appear in the UI

* **Detection:** CI. The `docker` job loads the DAG bag and asserts both DAG ids
  are present.
* **Why it is in CI:** an `ImportError` in a DAG file does not fail anything —
  the DAG just silently vanishes from the scheduler, and the first symptom is
  missing data days later.

### A run is still going when the next one starts

* **Automatic:** `max_active_runs=3` allows some overlap during catch-up;
  `execution_timeout=45min` kills a task that hangs.
* **Why not `depends_on_past=True`:** days are independent. One stuck day must
  not block every later day — that turns one missing day into a growing hole.

### A backfill floods the API

* **Automatic:** the `weather_api` Airflow pool caps concurrency at 6 across
  **both** DAGs, and the backfill DAG refuses ranges over 400 days.

### Nobody notices a failure at 03:00

* **Automatic:** `on_failure_callback` pages; SLA miss on `dbt_test` notifies.
* **Backstop:** `dbt source freshness` and `assert_no_missing_days` mean that
  even a completely missed alert surfaces as a failing test on a later run.
* **Design note:** two channels on purpose — `page` for "broken, act tonight",
  `notify` for "needs a data owner tomorrow". Mixing them makes the page channel
  worthless within a month.
