{{ config(
  materialized='table'
) }}

SELECT
    "Term" as "term", 
    "Year" as "year",
    "Grade" as "grade",
    "Meeting_Date" as "date",
    "Meeting_Type",
    "Number_Present",
    "Number_of_Parents",
    "Share_of_Parents_Engaged",
    "Attendance_Percentage_",
    "school_type"
FROM {{ ref("staging_parents_attendance") }}