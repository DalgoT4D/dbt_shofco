{{ config(
  materialized='table'
) }}

SELECT
    *
FROM {{ ref("staging_life_skills_training_session_details") }}