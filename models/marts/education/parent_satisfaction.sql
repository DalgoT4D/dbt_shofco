{{ config(
  materialized='table'
) }}

SELECT
    "term", "school", "education_satisfaction"
FROM {{ ref("staging_parents_satisfaction_survey") }}