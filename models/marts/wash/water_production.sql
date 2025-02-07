{{ config(
  materialized='table',
  tags=['wash_water_production', "wash"]
) }}

select
    date,
    status,
    bh_production,
    treated_consumption,
    tank,
    case
        when
            date is not NULL
            then EXTRACT(year from TO_DATE(date, 'DD/MM/YYYY'))
    end as year
from {{ ref('staging_water_production') }}
