/*
    Reconciliation between the two marts.

    The daily aggregate is built from the hourly fact, so any divergence means
    one of them was built from a stale run of the other - typically an
    incremental that was refreshed while the upstream was mid-load. Counting
    rows on both sides is cheap and catches it immediately.
*/

with hourly as (

    select
        city_id,
        observation_date,
        count(*) as hourly_rows
    from {{ ref('fct_weather_hourly') }}
    group by 1, 2

),

daily as (

    select
        city_id,
        observation_date,
        hours_observed
    from {{ ref('agg_weather_daily') }}

)

select
    coalesce(hourly.city_id, daily.city_id)                   as city_id,
    coalesce(hourly.observation_date, daily.observation_date) as observation_date,
    hourly.hourly_rows,
    daily.hours_observed
from hourly
full outer join daily
    on hourly.city_id = daily.city_id
   and hourly.observation_date = daily.observation_date
where hourly.hourly_rows is distinct from daily.hours_observed
