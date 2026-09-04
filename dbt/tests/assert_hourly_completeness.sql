{{ config(severity = 'warn', tags = ['completeness']) }}

/*
    Every city should have 24 hourly rows for every day that is finished.

    Warn, not error: the usual cause is the producer having published a partial
    day, which resolves itself on the next run once the restatement window picks
    the day up again. An error here would page someone for something that fixes
    itself, and that is how alerts get ignored.

    The most recent `source_lag_days + 1` days are excluded because they are
    legitimately still filling up.
*/

with daily as (

    select * from {{ ref('agg_weather_daily') }}

),

bounds as (

    select max(observation_date) as latest_date from daily

)

select
    daily.city_id,
    daily.observation_date,
    daily.hours_observed,
    {{ var('expected_hours_per_day') }} - daily.hours_observed as missing_hours
from daily
cross join bounds
where daily.observation_date < dateadd('day', -1, bounds.latest_date)
  and daily.hours_observed <> {{ var('expected_hours_per_day') }}
