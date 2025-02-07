{{ config(
  materialized='table',
  tags=['wash_mobiwater', "wash"]
) }}

SELECT DISTINCT
CAST("flow_device_id" AS INTEGER) as flow_device_id,
"flow_device_name" as flow_device_name
FROM {{ ref('staging_user_meters_mobiwater') }}
