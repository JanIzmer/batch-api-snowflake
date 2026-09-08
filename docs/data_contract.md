# Data contract

## What the contract is

[`contracts/weather_observation.v1.yml`](../contracts/weather_observation.v1.yml)
is the agreement between the producer (the Open-Meteo archive API) and this
pipeline. It is not documentation that describes the code — the code reads it.
`pipeline.contracts.load_contract()` parses the YAML and every landed batch is
validated against it, so the file and the behaviour cannot drift apart.

The contract fixes five things:

| Element | Value | Why it is in the contract |
|---|---|---|
| Grain | one row per `(city_id, observed_at_utc)` | Everything downstream assumes it; a change here silently doubles or halves every aggregate. |
| Primary key | `(city_id, observed_at_utc)` | It is the MERGE key. Idempotency is defined in terms of it. |
| Partition key | `observation_date` | The unit of reprocessing: one day of one city can be rebuilt alone. |
| Types and ranges | see `fields:` | Type is what the warehouse stores, range is what "plausible" means. |
| Source lag | 2 days | The producer publishes history late; without this the pipeline would ask for a day that does not exist yet and read the empty answer as data loss. |

## Types, and why nullability is per field

Every field is `required: true` or `required: false`, and the distinction is
deliberate:

* `city_id`, `latitude`, `longitude`, `observed_at_utc` are **required**. A row
  without them cannot be placed in the model at all — there is nowhere to put
  it. A null here rejects the row.
* The measurements (`temperature_2m_c`, `precipitation_mm`, …) are **nullable**.
  Weather stations have gaps, and the API returns `null` for them. Treating a
  gap as an error would reject perfectly good days for one missing hour; instead
  a null is stored, `agg_weather_daily.hours_missing_temperature` counts them,
  and averages ignore them.

Ranges are enforced too (`min`/`max`), because a type check alone accepts
`relative_humidity_2m_pct = 940.0`. A value outside the range is a rejected row,
not a rounded one — quietly clamping data is how a sensor fault becomes a
permanent, invisible bias.

## Metadata columns

The contract also declares the four columns the pipeline adds. They are the
contract with the *warehouse* rather than with the API:

| Column | Purpose |
|---|---|
| `_ingested_at_utc` | Source freshness (`dbt source freshness` reads it). |
| `_batch_id` | `sha256(city_id \| observation_date \| contract_version)`. Deterministic, so a re-run addresses exactly the same batch. |
| `_source_payload_hash` | `sha256` of the raw payload. Lets the loader tell "re-run of unchanged data" apart from "producer restated history". |
| `_contract_version` | Which contract produced the row, so two versions can coexist during a migration. |

## What happens when the schema changes

The compatibility policy is **backward**: a change is acceptable if code written
against version *n* still works against the new payload.

| Change at the producer | Breaking? | What the pipeline does | What a human does |
|---|---|---|---|
| New optional field appears | No | `quality.check_batch` logs `quality.unknown_fields` with the field name; the batch loads unchanged. | Decide whether it is wanted. If yes: add it to the contract, add it to RAW and staging, no version bump. |
| New **required** field appears | Yes | Rows still validate (we do not know about the field), but the model is now incomplete. | New contract version `v2`. |
| Field removed or renamed | Yes | The field becomes null → required fields reject, and the reject ratio crosses the 2% threshold → the task fails loudly. | New contract version; keep `v1` reading until history is migrated. |
| Type widened (`integer` → `float`) | No | Accepted: the contract's `float` already admits ints. | Nothing. |
| Type narrowed (`float` → `integer`) | Yes | Type violations → rejects → threshold breach → failure. | New contract version. |
| Unit or meaning changes (°C → °F) | **Yes, and invisible** | Nothing fails: 68.0 is a valid temperature. | This is the dangerous one. The only defences are the range tests and the `warn`-level `dbt_utils.accepted_range` checks on the marts, plus reading the producer's changelog. |
| Field reordered / payload key order changes | No | `payload_hash` sorts keys before hashing, so ordering alone does not look like a change. | Nothing. |

### Rolling out a new contract version

1. Add `contracts/weather_observation.v2.yml`. Never edit `v1` — history was
   landed under its rules, and rewriting them makes old rows unexplainable.
2. Create `RAW.WEATHER_OBSERVATION_V2` and land new data there. Both versions
   run in parallel.
3. Add `stg_weather__observations_v2`, then `union` the two in staging with an
   explicit mapping for the changed field.
4. Once the backfill of history into `v2` is finished, point staging at `v2`
   alone, mark `v1` `status: retired` and drop the old table after the retention
   window.

The contract version travels with every row (`_contract_version`), so "which
rule produced this number" is always answerable, including for rows loaded
years ago.

## Where the contract shows up downstream

* `dbt/models/staging/_sources.yml` — freshness thresholds are copied from the
  contract's `sla:` block.
* `dbt/models/staging/_stg_models.yml` — `accepted_values: [1]` on
  `contract_version` fails the build the moment an unannounced version appears
  in RAW.
* `dbt/models/marts/_marts_models.yml` — the range tests mirror the contract's
  `min`/`max`, so the same rule is checked on the way in (Python) and on the way
  out (SQL).
