{{ config(
    materialized='table',
    tags='wash_mobiwater'
) }}


SELECT DISTINCT
    "flowDeviceId" AS flow_device_id,
    "flowDeviceName" AS flow_device_name
FROM {{ source('staging_wash', 'User_Meters') }}
