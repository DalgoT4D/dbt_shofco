{{ config(
  materialized='table', 
  tags=['gender_life_skills_training',"gender"]
) }}

WITH raw_source AS (
    SELECT
        id,
        indexed_on,
        data::jsonb AS json_data,
        COALESCE(
            NULLIF(data::jsonb -> 'form' -> 'meta' ->> 'instanceID', ''),
            'missing_instance:' || id
        ) AS dedupe_key,
        COALESCE(
            (data::jsonb ->> 'received_on')::timestamp,
            indexed_on::timestamp
        ) AS received_on
    FROM {{ source('staging_gender', 'IIVC_Life_Skills_Training') }}
    WHERE
        data::jsonb ->> 'archived' IS NULL
        OR data::jsonb ->> 'archived' = 'false'
),

source_data AS (
    SELECT DISTINCT ON (dedupe_key)
        id,
        indexed_on,
        json_data
    FROM raw_source
    ORDER BY dedupe_key, received_on DESC, indexed_on DESC, id DESC
),

roc_club_participants AS (
    SELECT
        json_data -> 'form' ->> 'target_group' AS target_group,
        COALESCE(
            json_data -> 'form' ->> 'term',
            json_data -> 'form' -> 'school_information' ->> 'term'
        ) AS term,
        CASE
            WHEN
                COALESCE(
                    json_data -> 'form' ->> 'year',
                    json_data -> 'form' -> 'school_information' ->> 'year'
                ) = 'choice5'
                THEN '2024'
            ELSE COALESCE(
                json_data -> 'form' ->> 'year',
                json_data -> 'form' -> 'school_information' ->> 'year'
            )
        END AS year,
        json_data ->> 'received_on' AS form_filling_date,
        json_data -> 'form' -> 'meta' ->> 'instanceID' AS session_id,
        participant.value AS participant_data,
        participant.ordinality AS participant_ordinal,
        (json_data -> 'form' -> 'geographical_location' ->> 'ward') AS ward,
        (json_data -> 'form' -> 'geographical_location' ->> 'county') AS county_code,
        (json_data -> 'form' -> 'geographical_location' ->> 'constituency') AS constituency,
        (json_data -> 'form' -> 'meta' ->> 'username') AS assigned_to,
        CASE
            WHEN COALESCE(
                json_data -> 'form' ->> 'term',
                json_data -> 'form' -> 'school_information' ->> 'term'
            ) = 'term_1' THEN to_timestamp('2024-08-31', 'YYYY-MM-DD') 
            WHEN COALESCE(
                json_data -> 'form' ->> 'term',
                json_data -> 'form' -> 'school_information' ->> 'term'
            ) = 'term_2' THEN to_timestamp('2024-01-01', 'YYYY-MM-DD')
            WHEN COALESCE(
                json_data -> 'form' ->> 'term',
                json_data -> 'form' -> 'school_information' ->> 'term'
            ) = 'term3' THEN to_timestamp('2024-04-21', 'YYYY-MM-DD')
            ELSE NULL::timestamp
        END AS term_start_date,
        CASE
            WHEN COALESCE(
                json_data -> 'form' ->> 'term',
                json_data -> 'form' -> 'school_information' ->> 'term'
            ) = 'term_1' THEN to_timestamp('2024-12-31', 'YYYY-MM-DD') 
            WHEN COALESCE(
                json_data -> 'form' ->> 'term',
                json_data -> 'form' -> 'school_information' ->> 'term'
            ) = 'term_2' THEN to_timestamp('2024-04-20', 'YYYY-MM-DD')
            WHEN COALESCE(
                json_data -> 'form' ->> 'term',
                json_data -> 'form' -> 'school_information' ->> 'term'
            ) = 'term3' THEN to_timestamp('2024-08-31', 'YYYY-MM-DD')
            ELSE NULL::timestamp
        END AS term_end_date
    FROM source_data
    CROSS JOIN LATERAL jsonb_array_elements(
        json_data -> 'form' -> 'membership_details'
    ) WITH ORDINALITY AS participant(value, ordinality)
    WHERE
        json_data -> 'form' ->> 'target_group' = 'roc_club'
        AND jsonb_typeof(json_data -> 'form' -> 'membership_details') = 'array'
),

community_safe_space_participants AS (
    SELECT
        COALESCE(
            json_data -> 'form' ->> 'term',
            json_data -> 'form' -> 'school_information' ->> 'term'
        ) AS term,
        CASE
            WHEN
                COALESCE(
                    json_data -> 'form' ->> 'year',
                    json_data -> 'form' -> 'school_information' ->> 'year'
                ) = 'choice5'
                THEN '2024'
            ELSE COALESCE(
                json_data -> 'form' ->> 'year',
                json_data -> 'form' -> 'school_information' ->> 'year'
            )
        END AS year,
        NULL::timestamp AS term_start_date,
        NULL::timestamp AS term_end_date,
        json_data -> 'form' ->> 'target_group' AS target_group,
        json_data ->> 'received_on' AS form_filling_date,
        json_data -> 'form' -> 'meta' ->> 'instanceID' AS session_id,
        participant.value AS participant_data,
        participant.ordinality AS participant_ordinal,
        (json_data -> 'form' -> 'geographical_location' ->> 'ward') AS ward,
        (json_data -> 'form' -> 'geographical_location' ->> 'county') AS county_code,
        (json_data -> 'form' -> 'geographical_location' ->> 'constituency') AS constituency,
        (json_data -> 'form' -> 'meta' ->> 'username') AS assigned_to
    FROM source_data
    CROSS JOIN LATERAL jsonb_array_elements(
        json_data -> 'form' -> 'community_safe_space_participants_details'
    ) WITH ORDINALITY AS participant(value, ordinality)
    WHERE
        json_data -> 'form' ->> 'target_group' = 'community_safe_space'
        AND jsonb_typeof(json_data -> 'form' -> 'community_safe_space_participants_details') = 'array'
),

combined AS (
    SELECT
        target_group,
        term,
        year,
        term_start_date,
        term_end_date,
        form_filling_date,
        session_id,
        participant_ordinal,
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

    SELECT
        target_group,
        term,
        year,
        term_start_date,
        term_end_date,
        form_filling_date,
        session_id,
        participant_ordinal,
        COALESCE(
            participant_data ->> 'full_name_first_middle_surname',
            participant_data ->> 'full_name'
        ) AS participant_name,
        participant_data ->> 'gender' AS gender,
        ward,
        county_code,
        constituency,
        assigned_to
    FROM community_safe_space_participants
)

SELECT
    c.target_group,
    c.term,
    c.year,
    c.term_start_date,
    c.term_end_date,
    c.form_filling_date,
    c.session_id,
    c.participant_ordinal,
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
