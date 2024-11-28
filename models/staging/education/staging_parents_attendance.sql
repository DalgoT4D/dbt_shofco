{{ config(
  materialized='table'
) }}

SELECT
    *,
    'KSG' AS "school_type"
FROM {{ source('staging_education', 'KSG_Parents_Summary') }}
UNION ALL
SELECT
    *,
    'MSG' AS "school_type"
FROM {{ source('staging_education', 'MSG_Parents_Summary') }}