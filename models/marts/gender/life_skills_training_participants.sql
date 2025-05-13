{{ config(
  materialized='table',
  tags=['gender_life_skills_training', "gender"]
) }}

SELECT 
    participants.target_group,
    participants.term,
    participants.year,  
    {{ validate_date("term_start_date") }} AS term_start_date, 
    {{ validate_date("term_end_date") }} AS term_end_date,   
    {{ validate_date("form_filling_date") }} AS form_filling_date, 
    participants.session_id,  
    participants.participant_name,
    participants.gender,
    participants.ward as case_ward_name,
    participants.constituency as case_constituency_name,
    participants.county_code,
    participants.assigned_to,
    locations.county_name as county
FROM {{ ref("staging_life_skills_training_participant_details") }} as participants
left join
    {{ source("staging_gender", "dim_location_administrative_units") }} as locations
    on
        participants.county_code = locations.county_code
