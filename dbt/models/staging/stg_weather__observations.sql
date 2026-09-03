{{
    config(
        materialized = 'view',
        tags = ['staging', 'hourly']
    )
}}

/*
    Staging does four things and nothing else:
      - renames to the project's naming convention
      - casts to the types the marts expect
      - drops rows that cannot be modelled (no city, no timestamp)
      - collapses any accidental duplicate of the grain

    The QUALIFY is defensive. The MERGE in the loader already keys on
    (city_id, observed_at_utc), so duplicates should be impossible - but a
    manual backfill run with the wrong role, or a restored table, can reintroduce
    them, and a silently doubled fact is much more expensive than this filter.
*/

with source as (

    select * from {{ source('weather_raw', 'weather_observation') }}

),

renamed as (

    select
        city_id                                      as city_id,
        observed_at_utc::timestamp_ntz               as observed_at_utc,
        observation_date::date                       as observation_date,
        latitude::float                              as latitude,
        longitude::float                             as longitude,
        temperature_2m_c::float                      as temperature_c,
        relative_humidity_2m_pct::float              as relative_humidity_pct,
        precipitation_mm::float                      as precipitation_mm,
        wind_speed_10m_kmh::float                    as wind_speed_kmh,
        weather_code::int                            as weather_code,
        _ingested_at_utc::timestamp_ntz              as ingested_at_utc,
        _batch_id                                    as batch_id,
        _source_payload_hash                         as source_payload_hash,
        _contract_version::int                       as contract_version

    from source
    where city_id is not null
      and observed_at_utc is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['city_id', 'observed_at_utc']) }} as observation_key,
    *
from renamed
qualify row_number() over (
    partition by city_id, observed_at_utc
    order by ingested_at_utc desc
) = 1
