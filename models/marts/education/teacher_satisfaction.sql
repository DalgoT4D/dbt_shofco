{{ config(
  materialized='table',
    tags=["education_satisfaction_surveys", "education"]
) }}

SELECT
    teaching_level, 
    education_satisfaction,
    LOWER(school_name) AS school_type,
    year,
    {{ validate_date("submission_time") }} AS submission_time
FROM {{ ref("staging_teacher_satisfaction_survey") }}
