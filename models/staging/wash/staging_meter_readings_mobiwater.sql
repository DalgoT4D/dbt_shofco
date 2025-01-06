{{ config(
    materialized='incremental',
  tags='wash_mobiwater'
) }}

WITH source_data AS (
    SELECT
        "value",
        "flow_device_id",
        "_airbyte_raw_id",
        "_airbyte_extracted_at",
        "_airbyte_meta"
    FROM {{ source('staging_wash', 'Meter_Consumption') }}
),

deduplicated_source AS (
    SELECT DISTINCT
        "value",
        "flow_device_id",
        "_airbyte_raw_id",
        "_airbyte_extracted_at",
        "_airbyte_meta"
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