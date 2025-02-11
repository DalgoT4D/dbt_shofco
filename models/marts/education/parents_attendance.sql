{{ config(
  materialized='table',
    tags=["education_attendance", "education"]
) }}

SELECT
    "Term" as "term", 
    "Year" as "year",
    "Grade" as "grade",
    {{ validate_date("Meeting_Date") }} AS "date",
    "Meeting_Type" as "meeting_type", 
    "Number_Present" as "number_present",
    "Number_of_Parents" as "number_of_parents",
    "Share_of_Parents_Engaged" as "share_of_parents_engaged",
    "Attendance_Percentage_" as "attendance_percentage",
    LOWER("school_type") as "school_type"
FROM {{ ref("staging_parents_attendance") }}
