{{ config(
  materialized='table',
  tags='wash_water_production'
) }}

select
*
FROM {{ ref('staging_water_production') }}