{{ config(
    materialized='table',
    tags=['wash_mobiwater', "wash"]
) }}

SELECT DISTINCT
    value,
    flow_device_id,
    date
FROM {{ source('staging_wash', 'meter_consumption') }}
