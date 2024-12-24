{{ config(
  materialized='table',
  tags='wash_water_production'
) }}

WITH staging_data AS (
    SELECT
        CAST(_airbyte_extracted_at AS DATE) AS "date", 
        CAST("flow_device_id" AS NUMERIC) AS "flowdeviceid", 
        CAST("value" AS NUMERIC) AS "value" 
    FROM {{ ref('staging_meter_readings_mobiwater') }}
),

user_meters AS (
    SELECT
        CAST("flowDeviceId" AS NUMERIC) AS "flowdeviceid",
        "flowDeviceName" AS "flowdevicename"
    FROM {{ ref('user_meters_mobiwater') }}
)

SELECT DISTINCT
    s."date",
    s."flowdeviceid",
    u."flowdevicename",
    s."value"
FROM staging_data s
LEFT JOIN user_meters u
ON s."flowdeviceid" = u."flowdeviceid"