{{ config(
  materialized='table',
  tags=['gender_life_skills_training', "gender"]
) }}

SELECT DISTINCT
    session_deets.id,
    {{ validate_date("indexed_on") }} AS indexed_on,
    session_deets.form_name,
    session_deets.comments,
    session_deets.target_group,
    session_deets.user_id,
    session_deets.village_name,
    session_deets.community_safe_space_name,
    session_deets.num_girls_in_safe_space,
    session_deets.school_term,
    session_deets.school_year,
    session_deets.school_name,
    session_deets.type_of_school,
    session_deets.num_club_members,
    session_deets.life_skills_form_status,
    session_deets.patron_mobile_number,
    session_deets.ward as case_ward_name,
    session_deets.constituency as case_constituency_name,
    session_deets.county_code,
    session_deets.assigned_to,
    CASE 
        WHEN LENGTH(session_deets.county_code) <= 3 THEN locations.county_name
        ELSE INITCAP(session_deets.county_code)
    END as county

FROM {{ ref("staging_life_skills_training_session_details") }} as session_deets
left join
    {{ source("staging_gender", "dim_location_administrative_units") }} as locations
    on
        session_deets.county_code = locations.county_code
        AND LENGTH(session_deets.county_code) <= 3
