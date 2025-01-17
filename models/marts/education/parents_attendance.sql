{{ config(
  materialized='table',
    tags="education_attendance"
) }}

SELECT
    "Term" AS term, 
    "Year" AS year,
    "Grade" AS grade,
    "Meeting_Date" AS date,
    "Meeting_Type",
    "Number_Present",
    "Number_of_Parents",
    "Share_of_Parents_Engaged",
    "Attendance_Percentage_",
    school_type
FROM {{ ref("staging_parents_attendance") }}
