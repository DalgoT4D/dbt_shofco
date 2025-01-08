{{ config(
  materialized='table',
  tags='wash_mobiwater'
) }}

WITH staging_data AS (
    SELECT
        CAST("date" AS DATE), 
        CAST("flow_device_id" AS NUMERIC) AS "flow_device_id", 
        CAST("value" AS NUMERIC) AS "value" 
    FROM {{ ref('staging_meter_readings_mobiwater') }}
),

user_meters AS (
    SELECT
        CAST("flow_device_id" AS NUMERIC) AS "flow_device_id",
        "flow_device_name" AS "flow_device_name"
    FROM {{ ref('user_meters_mobiwater') }}
)

SELECT DISTINCT
    s."date",
    s."flow_device_id",
    u."flow_device_name",
    s."value"
FROM staging_data s
LEFT JOIN user_meters u
ON s."flow_device_id" = u."flow_device_id"