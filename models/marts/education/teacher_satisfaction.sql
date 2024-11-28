{{ config(
  materialized='table'
) }}

SELECT
    "teaching_level", "school_name", "education_satisfaction"
FROM {{ ref("staging_teacher_satisfaction_survey") }}