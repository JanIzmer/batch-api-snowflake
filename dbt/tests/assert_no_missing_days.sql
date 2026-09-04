{{ config(severity = 'warn', tags = ['completeness']) }}

/*
    A day with *no* rows at all is invisible to the completeness test above,
    because that test only looks at days that exist. This one walks the calendar
    between the first and last observation per city and reports the holes.

    A hole is almost always a failed Airflow run that nobody backfilled, which
    is exactly what the runbook's catch-up procedure is for.
*/

with per_city as (

    select
        city_id,
        min(observation_date) as first_date,
        max(observation_date) as last_date
    from {{ ref('agg_weather_daily') }}
    group by 1

),

calendar as (

    select
        per_city.city_id,
        dateadd('day', seq.value, per_city.first_date) as expected_date
    from per_city
    join table(generator(rowcount => 3650)) seq
      on dateadd('day', seq.value, per_city.first_date) <= per_city.last_date

),

actual as (

    select city_id, observation_date from {{ ref('agg_weather_daily') }}

)

select
    calendar.city_id,
    calendar.expected_date as missing_date
from calendar
left join actual
    on calendar.city_id = actual.city_id
   and calendar.expected_date = actual.observation_date
where actual.observation_date is null
