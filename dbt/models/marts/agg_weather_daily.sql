{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['city_id', 'observation_date'],
        on_schema_change = 'append_new_columns',
        cluster_by = ['observation_date'],
        tags = ['marts', 'daily'],
        post_hook = "{{ grant_select('ANALYST') }}"
    )
}}

/*
    Grain: one row per city per day.

    `is_complete_day` is the important column. A day assembled from fewer than
    24 hourly rows is still published - hiding it would look like missing data
    to a dashboard - but it is flagged so that consumers and the completeness
    test can tell "not finished yet" apart from "genuinely quiet".
*/

with hourly as (

    select * from {{ ref('fct_weather_hourly') }}

    {% if is_incremental() %}
    where {{ restatement_filter('observation_date') }}
    {% endif %}

)

select
    city_id,
    observation_date,
    any_value(city_name)                                  as city_name,
    any_value(country_code)                               as country_code,
    count(*)                                              as hours_observed,
    count(*) = {{ var('expected_hours_per_day') }}        as is_complete_day,
    round(avg(temperature_c), 2)                          as avg_temperature_c,
    round(min(temperature_c), 2)                          as min_temperature_c,
    round(max(temperature_c), 2)                          as max_temperature_c,
    round(avg(relative_humidity_pct), 2)                  as avg_relative_humidity_pct,
    round(sum(precipitation_mm), 2)                       as total_precipitation_mm,
    round(max(wind_speed_kmh), 2)                         as max_wind_speed_kmh,
    count_if(is_wet_hour)                                 as wet_hours,
    count_if(is_freezing_hour)                            as freezing_hours,
    count_if(temperature_c is null)                       as hours_missing_temperature,
    max(ingested_at_utc)                                  as last_ingested_at_utc,
    current_timestamp()                                   as dbt_updated_at
from hourly
group by city_id, observation_date
