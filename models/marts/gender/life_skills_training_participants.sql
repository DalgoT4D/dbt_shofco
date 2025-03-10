{{ config(
  materialized='table',
  tags=['gender_life_skills_training', "gender"]
) }}

SELECT 
    target_group,
    term,
    year,  
    {{ validate_date("term_start_date") }} AS term_start_date, 
    {{ validate_date("term_end_date") }} AS term_end_date,   
    {{ validate_date("form_filling_date") }} AS form_filling_date, 
    session_id,  
    participant_name,
    gender 
FROM {{ ref("staging_life_skills_training_participant_details") }}
