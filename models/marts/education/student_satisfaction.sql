{{ config(
  materialized='table'
) }}

SELECT
    "term", "school", "school_satisfaction"
FROM {{ ref("staging_student_satisfaction_survey") }}