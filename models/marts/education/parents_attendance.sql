{{ config(
  materialized='table',
    tags=["education_attendance", "education"]
) }}

SELECT
    "Term" AS term,
    "Year" AS year,
    "Grade" AS grade,
    "Meeting_Type" AS meeting_type,
    "Number_Present" AS number_present,
    "Number_of_Parents" AS number_of_parents,
    "Share_of_Parents_Engaged" AS share_of_parents_engaged,
    "Attendance_Percentage_" AS attendance_percentage,
    TO_DATE("Meeting_Date", 'DD/MM/YYYY') AS date,
    LOWER(school_type) AS school_type
FROM {{ ref("staging_parents_attendance") }}
