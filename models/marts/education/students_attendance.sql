{{ config(
  materialized='table',
    tags="education_attendance"
) }}

SELECT
    "Term" AS term,
    "Grade" AS grade,
    "Year" AS year,
    "Avg_Days_Attended",
    "Total_School_Days",
    "Number_of_Students",
    "Attendance_Percentage",
    "All_Students___Total_Days_Absent",
    "All_Students___Total_Days_Present",
    LOWER("School_Type") AS school_type
FROM {{ ref("staging_students_attendance") }}
