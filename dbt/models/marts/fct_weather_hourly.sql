{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'observation_key',
        on_schema_change = 'append_new_columns',
        cluster_by = ['observation_date'],
        tags = ['marts', 'hourly'],
        post_hook = "{{ grant_select('ANALYST') }}"
    )
}}

/*
    Grain: one row per city per hour.

    Incremental notes
    -----------------
    * `merge` on the surrogate key, not `insert_overwrite` or `append`: the
      producer restates recent history, and a re-run must correct rows rather
      than add a second copy of them.
    * The incremental filter reprocesses the last `restatement_window_days`
      days instead of "everything newer than max(date)". Cheap (a week of eight
      cities is ~1.3k rows) and it means a late correction is actually picked up.
    * `cluster_by = observation_date` because every dashboard query and every
      backfill filters on it; without it a date filter scans the whole table
      once the fact grows past a few hundred megabytes.
*/

with observations as (

    select * from {{ ref('stg_weather__observations') }}

    {% if is_incremental() %}
    where {{ restatement_filter('observation_date') }}
    {% endif %}

),

cities as (

    select * from {{ ref('stg_weather__cities') }}

)

select
    observations.observation_key,
    observations.city_id,
    cities.city_name,
    cities.country_code,
    observations.observed_at_utc,
    observations.observation_date,
    date_trunc('hour', observations.observed_at_utc)      as observed_hour_utc,
    hour(observations.observed_at_utc)                    as hour_of_day_utc,
    dayofweekiso(observations.observation_date)           as day_of_week_iso,
    observations.temperature_c,
    observations.relative_humidity_pct,
    observations.precipitation_mm,
    observations.wind_speed_kmh,
    observations.weather_code,
    observations.precipitation_mm > 0                     as is_wet_hour,
    observations.temperature_c < 0                        as is_freezing_hour,
    observations.batch_id,
    observations.source_payload_hash,
    observations.contract_version,
    observations.ingested_at_utc,
    current_timestamp()                                   as dbt_updated_at
from observations
inner join cities
    on observations.city_id = cities.city_id
