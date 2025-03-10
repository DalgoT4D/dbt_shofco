{{ config(
  materialized='table',
  tags=["education_attendance", "education"]
) }}

SELECT
    "Term" AS term, 
    "Year" AS year, 
    "Grade" AS grade, 
    "Avg_Days_Attended" AS avg_days_attended, 
    "Total_School_Days" AS total_school_days, 
    "Number_of_Students" AS number_of_students, 
    "Attendance_Percentage" AS attendance_percentage, 
    "All_Students___Total_Days_Absent" AS total_days_absent,
    "All_Students___Total_Days_Present" AS total_days_present, 
    'ksg' AS school_type
FROM {{ source('staging_education', 'KSG_Summary') }}
UNION ALL
SELECT
    "Term" AS term,  
    '2024' AS year, 
    "Grade" AS grade, 
    "Avg_Days_Attended" AS avg_days_attended, 
    "Total_School_Days" AS total_school_days, 
    "Number_of_Students" AS number_of_students, 
    "Attendance_Percentage" AS attendance_percentage, 
    "All_Students___Total_Days_Absent" AS total_days_absent,
    "All_Students___Total_Days_Present" AS total_days_present, 
    'msg' AS school_type
FROM {{ source('staging_education', 'MSG_Summary') }}
