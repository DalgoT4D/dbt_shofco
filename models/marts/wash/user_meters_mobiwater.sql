{{ config(
  materialized='table',
  tags=['wash_mobiwater', "wash"]
) }}

SELECT DISTINCT
    flow_device_name,
    CAST(flow_device_id AS INTEGER) AS flow_device_id
FROM {{ ref('staging_user_meters_mobiwater') }}
