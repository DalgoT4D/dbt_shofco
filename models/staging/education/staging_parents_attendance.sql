{{ config(
  materialized='table',
  tags=["education_attendance", "education"]
) }}

SELECT
    "Term" AS term,
    "Year" AS year,
    "Grade" AS grade,
    "Cohort" AS cohort,
    "Meeting_Date" AS meeting_date,
    "Meeting_Type" AS meeting_type,
    "Number_Present" AS number_present,
    "Number_of_Parents" AS number_of_parents,
    "Attendance_Percentage_" AS attendance_percentage,
    "Share_of_Parents_Engaged" AS share_of_parents_engaged,
    "Attendance_Data_Collected" AS attendance_data_collected,
    _airbyte_extracted_at,
    'ksg' AS school_type
FROM {{ source('staging_education', 'KSG_Parents_Summary') }}
WHERE "Attendance_Percentage_" <> '#DIV/0!'
UNION ALL
SELECT
    "Term" AS term,
    "Year" AS year,
    "Grade" AS grade,
    "Cohort" AS cohort,
    "Meeting_Date" AS meeting_date,
    "Meeting_Type" AS meeting_type,
    "Number_Present" AS number_present,
    "Number_of_Parents" AS number_of_parents,
    "Attendance_Percentage_" AS attendance_percentage,
    "Share_of_Parents_Engaged" AS share_of_parents_engaged,
    "Attendance_Data_Collected" AS attendance_data_collected,
    _airbyte_extracted_at,
    'msg' AS school_type
FROM {{ source('staging_education', 'MSG_Parents_Summary') }}
WHERE "Attendance_Percentage_" <> '#DIV/0!'
UNION ALL
SELECT
    "Term" AS term,
    "Year" AS year,
    "Grade" AS grade,
    "Cohort" AS cohort,
    "Meeting_Date" AS meeting_date,
    "Meeting_Type" AS meeting_type,
    "Number_Present" AS number_present,
    "Number_of_Parents" AS number_of_parents,
    "Attendance_Percentage_" AS attendance_percentage,
    "Share_of_Parents_Engaged" AS share_of_parents_engaged,
    "Attendance_Data_Collected" AS attendance_data_collected,
    _airbyte_extracted_at,
    'ksg' AS school_type
FROM {{ source('staging_education', 'KSG_Parents_Summary_2025') }}
WHERE "Attendance_Percentage_" <> '#DIV/0!'
UNION ALL
SELECT
    "Term" AS term,
    "Year" AS year,
    "Grade" AS grade,
    "Cohort" AS cohort,
    "Meeting_Date" AS meeting_date,
    "Meeting_Type" AS meeting_type,
    "Number_Present" AS number_present,
    "Number_of_Parents" AS number_of_parents,
    "Attendance_Percentage_" AS attendance_percentage,
    "Share_of_Parents_Engaged" AS share_of_parents_engaged,
    "Attendance_Data_Collected" AS attendance_data_collected,
    _airbyte_extracted_at,
    'msg' AS school_type
FROM {{ source('staging_education', 'MSG_Parents_Summary_2025') }}
WHERE "Attendance_Percentage_" <> '#DIV/0!'
