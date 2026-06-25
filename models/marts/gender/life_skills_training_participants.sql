{{ config(
  materialized='table',
  tags=['gender_life_skills_training', "gender"]
) }}

SELECT DISTINCT
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
    CASE 
        WHEN LOWER(participants.assigned_to) = 'wilson.onyango'     THEN 'wilson.obiero'
        WHEN LOWER(participants.assigned_to) = 'zena.khassim'       THEN 'zena.kassim'
        WHEN LOWER(participants.assigned_to) = 'damaris.walengwa'   THEN 'damaris.walegwa'
        WHEN LOWER(participants.assigned_to) = 'elphas.mtekwa'      THEN 'elphus.mtekwa'
        WHEN LOWER(participants.assigned_to) = 'triza.njeri'        THEN 'trizah.njeri'
        WHEN LOWER(participants.assigned_to) = 'wmwita'             THEN 'w.mwita'
        WHEN LOWER(participants.assigned_to) = 'eanas.makokha'      THEN 'esnas.makokha'
        WHEN LOWER(participants.assigned_to) = 'amina.katata'       THEN 'amina.katana'
        WHEN LOWER(participants.assigned_to) = 'emmaculate.achieng' THEN 'emma.achieng'
        ELSE participants.assigned_to
    END as assigned_to,
    CASE 
        WHEN LENGTH(participants.county_code) > 3 THEN REPLACE(INITCAP(participants.county_code), '_', ' ')
        ELSE REPLACE(locations.county_name, '_', ' ')
    END as county
FROM {{ ref("staging_life_skills_training_participant_details") }} as participants
left join
    {{ source("staging_gender", "dim_location_administrative_units") }} as locations
    on
        participants.county_code = locations.county_code