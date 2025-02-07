{{ config(
  materialized='table',
  tags=['gender_mental_health_assesment', "gender"]
) }}

SELECT
    case_id,
    sessions_attended,
    date_of_final_assessment,
    session_id
FROM {{ ref('staging_gender_final_mental_health_assesment') }}
WHERE sessions_attended IS NOT NULL  -- Filter to ensure there are valid session counts