{{ config(
  materialized='table',
  tags=["education_attendance", "education"]
) }}

SELECT
    "Term" as term, 
    "Year" as year, 
    "Grade" as grade, 
    "Avg_Days_Attended" as avg_days_attended, 
    "Total_School_Days" as total_school_days, 
    "Number_of_Students" as number_of_students, 
    "Attendance_Percentage" as attendance_percentage, 
    "All_Students___Total_Days_Absent" as total_days_absent,
    "All_Students___Total_Days_Present" as total_days_present, 
    'ksg' AS school_type
FROM {{ source('staging_education', 'KSG_Summary') }}
UNION ALL
SELECT
    "Term" as term,  
    '2024' as year, 
    "Grade" as grade, 
    "Avg_Days_Attended" as avg_days_attended, 
    "Total_School_Days" as total_school_days, 
    "Number_of_Students" as number_of_students, 
    "Attendance_Percentage" as attendance_percentage, 
    "All_Students___Total_Days_Absent" as total_days_absent,
    "All_Students___Total_Days_Present" as total_days_present, 
    'msg' AS school_type
FROM {{ source('staging_education', 'MSG_Summary') }}
