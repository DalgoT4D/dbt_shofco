{{ config(
  materialized='table',
  tags=["school_break_activities", "education"]
) }}

SELECT
    owner_id,
    name_of_training
FROM {{ ref('staging_school_break_activities') }}