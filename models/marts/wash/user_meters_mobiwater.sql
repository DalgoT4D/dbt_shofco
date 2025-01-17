{{ config(
  materialized='table',
  tags='wash_mobiwater'
) }}

SELECT DISTINCT
    CAST(flow_device_id AS INTEGER) AS flow_device_id,
    flow_device_name
FROM {{ ref('staging_user_meters_mobiwater') }}
