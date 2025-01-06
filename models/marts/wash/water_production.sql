{{ config(
  materialized='table',
  tags='wash_water_production'
) }}

select
    "date",
    "status",
    "bh_production",
    "treated_consumption",
    "tank"
FROM {{ ref('staging_water_production') }}