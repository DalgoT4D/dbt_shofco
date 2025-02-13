{{ config(
  materialized='table',
    tags=["education_attendance", "education"]
) }}

SELECT
    term, 
    year,
    grade,
    cohort,
    {{ validate_date("meeting_date") }} AS "date",
    meeting_type, 
    number_present,
    number_of_parents,
    share_of_parents_engaged,
    attendance_percentage,
    LOWER("school_type") as "school_type"
FROM {{ ref("staging_parents_attendance") }}
