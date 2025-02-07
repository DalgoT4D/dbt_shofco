{{ config(
  materialized='table',
  tags=['gender_life_skills_training', "gender"]
) }}

SELECT *
FROM {{ ref("staging_life_skills_training_participant_details") }}
