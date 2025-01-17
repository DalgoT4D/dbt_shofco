{{ config(
  materialized='table'
) }}

SELECT
    id,	
    indexed_on,
    form_name,
    comments,
    target_group,
    user_id,
    username,
    village_name,
    num_girls_in_safe_space,
    county,
    ward,
    constituency,
    school_term,
    school_year,
    school_name,
    type_of_school,
    num_club_members,
    life_skills_form_status,
    patron_mobile_number
FROM {{ ref("staging_life_skills_training_session_details") }}
