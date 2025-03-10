{{ config(
  materialized='table',
  tags=['wash_water_production', "wash"]
) }}

select
    {{ validate_date("date_extracted") }} as date,
    status,
    bh_production,
    treated_consumption,
    tank,
    case
        when {{ validate_date("date_extracted") }} is not NULL then EXTRACT(year from {{ validate_date("date_extracted") }})
    end as year
from {{ ref('staging_water_production') }}
