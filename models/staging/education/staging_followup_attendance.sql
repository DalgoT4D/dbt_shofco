{{ config(
  materialized='table',
  tags=["education_attendance", "education"]
) }}

SELECT
    "Date_" AS date,
    "Grade" AS grade,
    "Stream" AS stream,
    "Name_of_Student" AS name_of_student,
    "Reason_for_absenteesm" AS absence_causes,
    "Estimated_reporting_date" AS estimated_reporting_date,
    "_airbyte_extracted_at" AS _airbyte_extracted_at,
    'KSG' AS school_type
FROM {{ source('staging_education', 'KSG_Follow_Up_Social_Worker') }}
UNION ALL
SELECT
    "Date_" AS date,
    "Grade" AS grade,
    "Stream" AS stream,
    "Name_of_Student" AS name_of_student,
    "Reason_for_absenteeIsm" AS absence_causes,
    "Estimated_reporting_date" AS estimated_reporting_date,
    "_airbyte_extracted_at" AS _airbyte_extracted_at,
    'MSG' AS school_type
FROM {{ source('staging_education', 'MSG_Follow_Up_Social_Worker') }}
