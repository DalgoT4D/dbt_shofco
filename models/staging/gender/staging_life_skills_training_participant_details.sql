{{ config(
  materialized='table', 
  tags=['gender_life_skills_training',"gender"]
) }}

WITH roc_club_participants AS (
    SELECT
        data::jsonb -> 'form' ->> 'target_group' AS target_group,
        COALESCE(
            data::jsonb -> 'form' ->> 'term',
            data::jsonb -> 'form' -> 'school_information' ->> 'term'
        ) AS term,
        CASE
            WHEN
                COALESCE(
                    data::jsonb -> 'form' ->> 'year',
                    data::jsonb -> 'form' -> 'school_information' ->> 'year'
                ) = 'choice5'
                THEN '2024'
            ELSE COALESCE(
                data::jsonb -> 'form' ->> 'year',
                data::jsonb -> 'form' -> 'school_information' ->> 'year'
            )
        END AS year,
        data::jsonb ->> 'received_on' AS form_filling_date,
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS session_id,
        jsonb_array_elements(
            data -> 'form' -> 'membership_details'
        ) AS participant_data,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'ward') AS ward,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'county') AS county_code,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'constituency') AS constituency,
        (data::jsonb -> 'form' -> 'meta' ->> 'username') AS assigned_to,
        CASE
            WHEN COALESCE(
                data::jsonb -> 'form' ->> 'term',
                data::jsonb -> 'form' -> 'school_information' ->> 'term'
            ) = 'term_1' THEN to_timestamp('2024-08-31', 'YYYY-MM-DD') 
            WHEN COALESCE(
                data::jsonb -> 'form' ->> 'term',
                data::jsonb -> 'form' -> 'school_information' ->> 'term'
            ) = 'term_2' THEN to_timestamp('2024-01-01', 'YYYY-MM-DD')
            WHEN COALESCE(
                data::jsonb -> 'form' ->> 'term',
                data::jsonb -> 'form' -> 'school_information' ->> 'term'
            ) = 'term3' THEN to_timestamp('2024-04-21', 'YYYY-MM-DD')
            ELSE NULL::timestamp
        END AS term_start_date,
        CASE
            WHEN COALESCE(
                data::jsonb -> 'form' ->> 'term',
                data::jsonb -> 'form' -> 'school_information' ->> 'term'
            ) = 'term_1' THEN to_timestamp('2024-12-31', 'YYYY-MM-DD') 
            WHEN COALESCE(
                data::jsonb -> 'form' ->> 'term',
                data::jsonb -> 'form' -> 'school_information' ->> 'term'
            ) = 'term_2' THEN to_timestamp('2024-04-20', 'YYYY-MM-DD')
            WHEN COALESCE(
                data::jsonb -> 'form' ->> 'term',
                data::jsonb -> 'form' -> 'school_information' ->> 'term'
            ) = 'term3' THEN to_timestamp('2024-08-31', 'YYYY-MM-DD')
            ELSE NULL::timestamp
        END AS term_end_date
    FROM {{ source('staging_gender', 'IIVC_Life_Skills_Training') }}
    WHERE
        data::jsonb->'form'->>'target_group' = 'roc_club'
        AND jsonb_typeof(data->'form'->'membership_details') = 'array'
        AND (data::jsonb->>'archived' IS NULL OR data::jsonb->>'archived' = 'false')
),

community_safe_space_participants AS (
    SELECT
        COALESCE(
            data::jsonb -> 'form' ->> 'term',
            data::jsonb -> 'form' -> 'school_information' ->> 'term'
        ) AS term,
        CASE
            WHEN
                COALESCE(
                    data::jsonb -> 'form' ->> 'year',
                    data::jsonb -> 'form' -> 'school_information' ->> 'year'
                ) = 'choice5'
                THEN '2024'
            ELSE COALESCE(
                data::jsonb -> 'form' ->> 'year',
                data::jsonb -> 'form' -> 'school_information' ->> 'year'
            )
        END AS year,
        NULL::timestamp AS term_start_date,
        NULL::timestamp AS term_end_date,
        data::jsonb -> 'form' ->> 'target_group' AS target_group,
        data::jsonb ->> 'received_on' AS form_filling_date,
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS session_id,
        jsonb_array_elements(
            data -> 'form' -> 'community_safe_space_participants_details'
        ) AS participant_data,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'ward') AS ward,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'county') AS county_code,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'constituency') AS constituency,
        (data::jsonb -> 'form' -> 'meta' ->> 'username') AS assigned_to
    FROM {{ source('staging_gender', 'IIVC_Life_Skills_Training') }}
    WHERE
        data::jsonb->'form'->>'target_group' = 'community_safe_space'
        AND jsonb_typeof(data->'form'->'community_safe_space_participants_details') = 'array'
        AND (data::jsonb->>'archived' IS NULL OR data::jsonb->>'archived' = 'false')
),

combined AS (
    SELECT DISTINCT
        target_group,
        term,
        year,
        term_start_date,
        term_end_date,
        form_filling_date,
        session_id,
        COALESCE(
            participant_data ->> 'member_full_names',
            participant_data ->> 'member_full_names_first_middle_surname'
        ) AS participant_name,
        participant_data ->> 'member_gender' AS gender,
        ward,
        county_code,
        constituency,
        assigned_to
    FROM roc_club_participants

    UNION ALL

    SELECT DISTINCT
        target_group,
        term,
        year,
        term_start_date,
        term_end_date,
        form_filling_date,
        session_id,
        participant_data ->> 'full_name_first_middle_surname' AS participant_name,
        participant_data ->> 'gender' AS gender,
        ward,
        county_code,
        constituency,
        assigned_to
    FROM community_safe_space_participants
)

SELECT DISTINCT
    c.target_group,
    c.term,
    c.year,
    c.term_start_date,
    c.term_end_date,
    c.form_filling_date,
    c.session_id,
    c.participant_name,
    c.gender,
    c.ward,
    c.county_code,
    c.constituency,
    c.assigned_to,
    -- Normalize county: handle short codes, underscores, hyphens, mixed case
    CASE
        WHEN LENGTH(TRIM(c.county_code)) <= 3
            THEN wl.county_name
        ELSE INITCAP(REPLACE(REPLACE(TRIM(c.county_code), '_', ' '), '-', ' '))
    END as county
FROM combined c
LEFT JOIN {{ source('staging_gender', 'ward_lookup') }} wl
    ON UPPER(TRIM(c.county_code)) = UPPER(wl.county_code)