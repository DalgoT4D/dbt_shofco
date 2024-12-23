{{ config(
  materialized='table',
  tags="education_attendance"
) }}

SELECT
    *,
    'KSG' AS "school_type"
FROM {{ source('staging_education', 'KSG_Follow_Up_Social_Worker') }}
UNION ALL
SELECT
    *,
    'MSG' AS "school_type"
FROM {{ source('staging_education', 'MSG_Follow_Up_Social_Worker') }}