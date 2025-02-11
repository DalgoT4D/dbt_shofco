{{ config(
    materialized='table',
    tags=['wash_mobiwater', "wash"]
) }}

SELECT DISTINCT
    value,
    flow_device_id,
    {{ validate_date ("date") }} AS date
FROM {{ source('staging_wash', 'meter_consumption') }}
