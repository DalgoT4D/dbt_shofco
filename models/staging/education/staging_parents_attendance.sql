{{ config(
  materialized='table',
  tags=["education_attendance", "education"]
) }}

SELECT
    "Term" as term,
    "Year" as year,
    "Grade" as grade,
    "Cohort" as cohort,
    "Meeting_Date" as meeting_date,
    "Meeting_Type" as meeting_type,
    "Number_Present" as number_present,
    "Number_of_Parents" as number_of_parents,
    "Attendance_Percentage_" as attendance_percentage,
    "Share_of_Parents_Engaged" as share_of_parents_engaged,
    "Attendance_Data_Collected" as attendance_data_collected,
    "_airbyte_extracted_at" AS _airbyte_extracted_at,
    'ksg' AS school_type
FROM {{ source('staging_education', 'KSG_Parents_Summary') }}
UNION ALL
SELECT
    "Term" as term,
    "Year" as year,
    "Grade" as grade,
    "Cohort" as cohort,
    "Meeting_Date" as meeting_date,
    "Meeting_Type" as meeting_type,
    "Number_Present" as number_present,
    "Number_of_Parents" as number_of_parents,
    "Attendance_Percentage_" as attendance_percentage,
    "Share_of_Parents_Engaged" as share_of_parents_engaged,
    "Attendance_Data_Collected" as attendance_data_collected,
    "_airbyte_extracted_at" AS _airbyte_extracted_at,
    'msg' AS school_type
FROM {{ source('staging_education', 'MSG_Parents_Summary') }}
