{{ config(
  materialized='table',
  tags=['wash_mobiwater', "wash"]
) }}

WITH staging_data AS (
    SELECT
        CAST(date AS DATE),
        CAST(flow_device_id AS NUMERIC) AS flow_device_id,
        CAST(value AS NUMERIC) AS value
    FROM {{ ref('staging_meter_readings_mobiwater') }}
),

user_meters AS (
    SELECT
        flow_device_name,
        CAST(flow_device_id AS NUMERIC) AS flow_device_id
    FROM {{ ref('user_meters_mobiwater') }}
)

SELECT DISTINCT
    s.date,
    s.flow_device_id,
    u.flow_device_name,
    s.value,
    CASE
        WHEN s.date IS NOT NULL THEN EXTRACT(YEAR FROM s.date)
    END AS year
FROM staging_data AS s
LEFT JOIN user_meters AS u
    ON s.flow_device_id = u.flow_device_id
