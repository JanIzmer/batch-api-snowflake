/*
    Nothing may be observed in the future.

    This catches the class of bug where a timezone is applied twice, or where a
    naive timestamp is read as local time and shifted forward. Those are silent
    - the row count looks fine - so they only surface as a test.

    Severity is error: a future row will also poison "latest reading" queries.
*/

select
    city_id,
    observed_at_utc,
    current_timestamp() as checked_at
from {{ ref('fct_weather_hourly') }}
where observed_at_utc > dateadd('hour', 1, current_timestamp())
