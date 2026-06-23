{{ config(
  materialized='table', 
  tags=['gender_life_skills_training', "gender"]
) }}

WITH source_data AS (
    SELECT DISTINCT
        id,
        indexed_on,
        data::jsonb AS json_data
    FROM {{ source('staging_gender', 'IIVC_Life_Skills_Training') }}
    WHERE
        data::jsonb ->> 'archived' IS NULL
        OR data::jsonb ->> 'archived' = 'false'
),

session_details AS (
    SELECT DISTINCT
        id,
        indexed_on,
        json_data -> 'form' ->> '@name' AS form_name,
        json_data -> 'form' ->> 'comments' AS comments,
        json_data -> 'form' ->> 'target_group' AS target_group,
        json_data -> 'form' -> 'meta' ->> 'userID' AS user_id,
        json_data -> 'form' -> 'meta' ->> 'username' AS assigned_to,
        json_data
        -> 'form'
        -> 'community_information'
        ->> 'name_of_village' AS village_name,
        json_data
        -> 'form'
        -> 'community_information'
        ->> 'name_of_the_community_safe_space' AS community_safe_space_name,
        json_data
        -> 'form'
        -> 'community_information'
        ->> 'number_of_girls_in_the_community_safe_space' AS num_girls_in_safe_space,
        -- Store raw county code for normalization
        json_data -> 'form' -> 'geographical_location' ->> 'county' AS county_code,
        json_data -> 'form' -> 'geographical_location' ->> 'ward' AS ward,
        json_data
        -> 'form'
        -> 'geographical_location'
        ->> 'constituency' AS constituency,
        COALESCE(
            json_data -> 'form' ->> 'term',
            json_data -> 'form' -> 'school_information' ->> 'term'
        ) AS school_term,
        COALESCE(
            json_data -> 'form' ->> 'year',
            json_data -> 'form' -> 'school_information' ->> 'year'
        ) AS school_year,
        json_data
        -> 'form'
        -> 'school_information'
        ->> 'school_name' AS school_name,
        json_data
        -> 'form'
        -> 'school_information'
        ->> 'type_of_school' AS type_of_school,
        COALESCE(
            json_data -> 'form' ->> 'number_of_club_membership',
            json_data -> 'form' -> 'school_information' ->> 'number_of_club_membership'
        ) AS num_club_members,
        json_data -> 'form' ->> 'life_skills_form' AS life_skills_form_status,
        json_data -> 'form' ->> 'club_patron_mobile_number' AS patron_mobile_number
    FROM source_data
)

SELECT DISTINCT
    s.id,
    s.indexed_on,
    s.form_name,
    s.comments,
    s.target_group,
    s.user_id,
    s.assigned_to,
    s.village_name,
    s.community_safe_space_name,
    s.num_girls_in_safe_space,
    s.county_code,
    s.ward,
    s.constituency,
    s.school_term,
    s.school_year,
    s.school_name,
    s.type_of_school,
    s.num_club_members,
    s.life_skills_form_status,
    s.patron_mobile_number,
    -- Normalize county: handle short codes, underscores, hyphens, mixed case
    CASE
        WHEN LENGTH(TRIM(s.county_code)) <= 3
            THEN wl.county_name
        ELSE INITCAP(REPLACE(REPLACE(TRIM(s.county_code), '_', ' '), '-', ' '))
    END as county
FROM session_details s
LEFT JOIN {{ source('staging_gender', 'ward_lookup') }} wl
    ON UPPER(TRIM(s.county_code)) = UPPER(wl.county_code)