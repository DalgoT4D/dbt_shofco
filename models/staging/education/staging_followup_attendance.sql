{{ config(
  materialized='table',
  tags=["education_attendance", "education"]
) }}

SELECT
    "Date_" AS date_of_absence,
    "Grade" AS grade,
    "Stream" AS stream,
    "Name_of_Student" AS name_of_student,
    "Reason_for_absenteesm" AS absence_causes,
    "Estimated_reporting_date" AS estimated_reporting_date,
    _airbyte_extracted_at,
    'KSG' AS school_type,
    '2024' AS year
FROM {{ source('staging_education', 'KSG_Follow_Up_Social_Worker') }}
UNION ALL
SELECT
    "Date_" AS date_of_absence,
    "Grade" AS grade,
    "Stream" AS stream,
    "Name_of_Student" AS name_of_student,
    "Reason_for_absenteeIsm" AS absence_causes,
    "Estimated_reporting_date" AS estimated_reporting_date,
    _airbyte_extracted_at,
    'MSG' AS school_type,
    '2024' AS year
FROM {{ source('staging_education', 'MSG_Follow_Up_Social_Worker') }}
UNION ALL
SELECT
    "Date_" AS date_of_absence,
    "Grade" AS grade,
    "Stream" AS stream,
    "Name_of_Student" AS name_of_student,
    "Reason_for_absenteeIsm" AS absence_causes,
    "Estimated_reporting_date" AS estimated_reporting_date,
    _airbyte_extracted_at,
    'MSG' AS school_type,
    '2025' AS year
FROM {{ source('staging_education', 'MSG_Follow_Up_Social_Worker_2025') }}
UNION ALL
SELECT
    "Date_" AS date_of_absence,
    "Grade" AS grade,
    "Stream" AS stream,
    "Name_of_Student" AS name_of_student,
    "Reason_for_absenteesm" AS absence_causes,
    "Estimated_reporting_date" AS estimated_reporting_date,
    _airbyte_extracted_at,
    'KSG' AS school_type,
    '2025' AS year
FROM {{ source('staging_education', 'KSG_Follow_Up_Social_Worker_2025') }}
