{{ config(
  materialized='table',
    tags=["education_attendance", "education"]
) }}

SELECT
    term,
    grade,
    year,
    avg_days_attended,
    total_school_days,
    number_of_students,
    attendance_percentage,
    total_days_absent,
    total_days_present,
    school_type
FROM {{ ref("staging_students_attendance") }}
