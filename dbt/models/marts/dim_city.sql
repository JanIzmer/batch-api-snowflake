{{
    config(
        materialized = 'table',
        tags = ['marts', 'daily'],
        post_hook = "{{ grant_select('ANALYST') }}"
    )
}}

/*
    Small and fully rebuilt every run: the catalogue is a handful of rows, so
    an incremental strategy here would cost more in complexity than it saves in
    credits.
*/

with cities as (

    select * from {{ ref('stg_weather__cities') }}

),

coverage as (

    select
        city_id,
        min(observation_date) as first_observation_date,
        max(observation_date) as last_observation_date,
        count(*)              as observation_count
    from {{ ref('stg_weather__observations') }}
    group by 1

)

select
    cities.city_id,
    cities.city_name,
    cities.country_code,
    cities.latitude,
    cities.longitude,
    cities.timezone_name,
    cities.population,
    cities.size_band,
    coverage.first_observation_date,
    coverage.last_observation_date,
    coalesce(coverage.observation_count, 0) as observation_count,
    coverage.city_id is not null            as has_observations
from cities
left join coverage using (city_id)
