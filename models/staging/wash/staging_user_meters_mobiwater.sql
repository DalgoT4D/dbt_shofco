{{ config(
    materialized='incremental'
) }}

WITH source_data AS (
    SELECT
        "deviceId",
        "deviceType",
        "flowDeviceId",
        "hardwareState",
        "flowDeviceName",
        "flowDeviceSize",
        "dailyConsumption",
        "flowDeviceLocation",
        "flowDevicePulseRate",
        "consumptionThreshold",
        "flowDeviceDescription",
        "_airbyte_raw_id",
        "_airbyte_extracted_at",
        -- Extract fields from `lastReceivedFlowData` JSON
        ("lastReceivedFlowData"::jsonb->>'rawPulses')::NUMERIC AS "rawPulses",
        ("lastReceivedFlowData"::jsonb->>'flowDataId')::NUMERIC AS "flowDataId",
        ("lastReceivedFlowData"::jsonb->>'measuredAt')::TIMESTAMP AS "measuredAt",
        ("lastReceivedFlowData"::jsonb->>'measuredFrom')::TIMESTAMP AS "measuredFrom",
        ("lastReceivedFlowData"::jsonb->>'flowDeviceState') AS "flowDeviceState",
        ("lastReceivedFlowData"::jsonb->>'acceptedFlowRate')::NUMERIC AS "acceptedFlowRate",
        ("lastReceivedFlowData"::jsonb->>'measuredFlowRate')::NUMERIC AS "measuredFlowRate",
        ("lastReceivedFlowData"::jsonb->>'notificationState') AS "notificationState",
        ("lastReceivedFlowData"::jsonb->>'predictedFlowRate')::NUMERIC AS "predictedFlowRate",
        ("lastReceivedFlowData"::jsonb->>'acceptedConsumption')::NUMERIC AS "acceptedConsumption",
        ("lastReceivedFlowData"::jsonb->>'measuredConsumption')::NUMERIC AS "measuredConsumption",
        ("lastReceivedFlowData"::jsonb->>'predictedConsumption')::NUMERIC AS "predictedConsumption",
        -- Extract fields from `organization` JSON
        ("organization"::jsonb->>'organizationId')::NUMERIC AS "organizationId",
        ("organization"::jsonb->>'organizationName') AS "organizationName",
        ("organization"::jsonb->>'organizationEmail') AS "organizationEmail",
        ("organization"::jsonb->>'organizationPhoneNo') AS "organizationPhoneNo",
        ("organization"::jsonb->>'organizationCallbackUrl') AS "organizationCallbackUrl"
    FROM {{ source('staging_wash', 'User_Meters') }}
),

deduplicated_source AS (
    SELECT DISTINCT
        "deviceId",
        "deviceType",
        "flowDeviceId",
        "hardwareState",
        "flowDeviceName",
        "flowDeviceSize",
        "dailyConsumption",
        "flowDeviceLocation",
        "flowDevicePulseRate",
        "consumptionThreshold",
        "flowDeviceDescription",
        "_airbyte_raw_id",
        "_airbyte_extracted_at",
        "rawPulses",
        "flowDataId",
        "measuredAt",
        "measuredFrom",
        "flowDeviceState",
        "acceptedFlowRate",
        "measuredFlowRate",
        "notificationState",
        "predictedFlowRate",
        "acceptedConsumption",
        "measuredConsumption",
        "predictedConsumption",
        "organizationId",
        "organizationName",
        "organizationEmail",
        "organizationPhoneNo",
        "organizationCallbackUrl"
    FROM source_data
)

{% if is_incremental() %}
SELECT *
FROM deduplicated_source
WHERE "_airbyte_extracted_at" > (SELECT MAX("_airbyte_extracted_at") FROM {{ this }})
{% else %}
SELECT *
FROM deduplicated_source
{% endif %}