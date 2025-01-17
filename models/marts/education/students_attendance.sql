{{ config(
  materialized='table',
    tags="education_attendance"
) }}

SELECT
    "Term" AS term,
    "Grade" AS grade,
    "Year" AS year,
    "Avg_Days_Attended" AS avg_days_attended,
    "Total_School_Days" AS total_school_days,
    "Number_of_Students" AS number_of_students,
    "Attendance_Percentage" AS attendance_percentage,
    "All_Students___Total_Days_Absent" AS total_days_absent,
    "All_Students___Total_Days_Present" AS total_days_present,
    LOWER("School_Type") AS school_type
FROM {{ ref("staging_students_attendance") }}
