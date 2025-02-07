{{ config(
  materialized='table',
  tags=["education_attendance", "education"]
) }}

SELECT
    "Term",
    "Year",
    "Grade",
    "Avg_Days_Attended",
    "Total_School_Days",
    "Number_of_Students",
    "Attendance_Percentage",
    "All_Students___Total_Days_Absent",
    "All_Students___Total_Days_Present",
    'KSG' AS "School_Type"
FROM {{ source('staging_education', 'KSG_Summary') }}
UNION ALL
SELECT
    "Term",
    '2024' AS "Year",
    "Grade",
    "Avg_Days_Attended",
    "Total_School_Days",
    "Number_of_Students",
    "Attendance_Percentage",
    "All_Students___Total_Days_Absent",
    "All_Students___Total_Days_Present",
    'MSG' AS "School_Type"
FROM {{ source('staging_education', 'MSG_Summary') }}
