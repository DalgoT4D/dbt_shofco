{{ config(
  materialized='table',
    tags="education_attendance"
) }}

SELECT
    "Term" as "term",
    "Grade" as "grade",
    "Year" as "year",
    "Avg_Days_Attended" as "avg_days_attended",
    "Total_School_Days" as "total_school_days",
    "Number_of_Students" as "number_of_students",
    "Attendance_Percentage" as "attendance_percentage",
    "All_Students___Total_Days_Absent" as "total_days_absent",
    "All_Students___Total_Days_Present" as "total_days_present",
    LOWER("School_Type") as "school_type"
FROM {{ ref("staging_students_attendance") }}