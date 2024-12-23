{{ config(
  materialized='table',
    tags="education_satisfaction_surveys"
) }}

SELECT
    "teaching_level", "school_name", "education_satisfaction",
    "school_name" AS "school_type", "year"
FROM {{ ref("staging_teacher_satisfaction_survey") }}