{{ config(
  materialized='table',
  tags="education_attendance"
) }}

SELECT
    "Date_",
    "Grade",
    "Stream",
    "Name_of_Student",
    "Reason_for_absenteesm",
    "Estimated_reporting_date",
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta",
    'KSG' AS "school_type"
FROM {{ source('staging_education', 'KSG_Follow_Up_Social_Worker') }}
UNION ALL
SELECT
    "Date_",
    "Grade",
    "Stream",
    "Name_of_Student",
    "Reason_for_absenteesm",
    "Estimated_reporting_date",
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta",
    'MSG' AS "school_type"
FROM {{ source('staging_education', 'MSG_Follow_Up_Social_Worker') }}