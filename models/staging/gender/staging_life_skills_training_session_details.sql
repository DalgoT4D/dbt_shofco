{{ config(
  materialized='table'
) }}

WITH source_data AS (
    SELECT DISTINCT
        id,
        indexed_on,
        data::jsonb AS json_data
    FROM {{ source('staging_gender', 'IIVC_Life_Skills_Training') }}
)
SELECT
    id,
    indexed_on,
    json_data->'form'->>'@name' AS form_name,  
    json_data->'form'->>'comments' AS comments,
    json_data->'form'->>'target_group' AS target_group,
    json_data->'form'->'meta'->>'userID' AS user_id,
    json_data->'form'->'meta'->>'username' AS username,
    
    -- Community Information
    json_data->'form'->'community_information'->>'name_of_village' AS village_name,
    json_data->'form'->'community_information'->>'name_of_the_community_safe_space' AS community_safe_space_name,
    json_data->'form'->'community_information'->>'number_of_girls_in_the_community_safe_space' AS num_girls_in_safe_space,

    -- Geographical Information
    json_data->'form'->'geographical_location'->>'county' AS county,
    json_data->'form'->'geographical_location'->>'ward' AS ward,
    json_data->'form'->'geographical_location'->>'constituency' AS constituency,

    -- School Information
    json_data->'form'->'school_information'->>'term' AS school_term,
    json_data->'form'->'school_information'->>'year' AS school_year,
    json_data->'form'->'school_information'->>'school_name' AS school_name,
    json_data->'form'->'school_information'->>'type_of_school' AS type_of_school,
    json_data->'form'->'school_information'->>'number_of_club_membership' AS num_club_members,

    -- Other relevant details
    json_data->'form'->'life_skills_form' AS life_skills_form_status,
    json_data->'form'->'club_patron_mobile_number' AS patron_mobile_number

FROM source_data