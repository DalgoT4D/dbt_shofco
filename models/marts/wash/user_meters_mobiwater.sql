{{ config(
  materialized='table',
  tags='wash_water_production'
) }}

SELECT DISTINCT
CAST("flowDeviceId" AS INTEGER),
"flowDeviceName"
FROM {{ ref('staging_user_meters_mobiwater') }}