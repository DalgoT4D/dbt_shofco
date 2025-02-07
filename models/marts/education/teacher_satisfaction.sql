{{ config(
  materialized='table',
    tags=["education_satisfaction_surveys", "education"]
) }}

SELECT
    teaching_level,
    education_satisfaction,
    year,
    LOWER(school_name) AS school_type
FROM {{ ref("staging_teacher_satisfaction_survey") }}
