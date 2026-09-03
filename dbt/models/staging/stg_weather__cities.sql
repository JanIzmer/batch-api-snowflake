{{
    config(
        materialized = 'view',
        tags = ['staging']
    )
}}

with source as (

    select * from {{ ref('city_catalogue') }}

)

select
    city_id,
    name                       as city_name,
    country_code,
    latitude,
    longitude,
    timezone_name,
    population,
    case
        when population >= 2000000 then 'large'
        when population >= 1000000 then 'medium'
        else 'small'
    end                        as size_band
from source
