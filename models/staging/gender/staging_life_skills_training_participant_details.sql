{{ config(
  materialized='table', 
  tags=['gender_life_skills_training',"gender"]
) }}

WITH roc_club_participants AS (
    SELECT
        data::jsonb -> 'form' ->> 'target_group' AS target_group,
        -- Extract term from either location (new format first, then old format)
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
        -- Use 'received_on' for the form-filling date
        data::jsonb ->> 'received_on' AS form_filling_date,
        -- Extract the session ID
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS session_id,
        jsonb_array_elements(
            data -> 'form' -> 'membership_details'
        ) AS participant_data,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'ward') AS ward,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'county') AS county_code,
        (data::jsonb -> 'form' -> 'geographical_location' ->> 'constituency') AS constituency,
        (data::jsonb -> 'form' -> 'meta' ->> 'username') AS assigned_to,
        -- Assign start and end dates for each term as timestamp
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
        AND jsonb_typeof(data->'form'->'membership_details') = 'array'  -- Ensure it's an array
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
        NULL::timestamp AS term_start_date,-- Use NULL for timestamp fields to maintain consistency with ROC Club
        NULL::timestamp AS term_end_date,
        data::jsonb -> 'form' ->> 'target_group' AS target_group,
        data::jsonb ->> 'received_on' AS form_filling_date,-- Use 'received_on' for the form-filling date
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS session_id,-- Extract the session ID
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
        AND jsonb_typeof(data->'form'->'community_safe_space_participants_details') = 'array'  -- Ensure it's an array
        AND (data::jsonb->>'archived' IS NULL OR data::jsonb->>'archived' = 'false')
)

-- Combine the participants from both roc_club and community_safe_space
SELECT DISTINCT
    target_group,
    term,
    year,  -- Year extracted from JSON with choice5 -> 2024 conversion
    term_start_date,  -- Start date based on the term or NULL
    term_end_date,    -- End date based on the term or NULL
    form_filling_date,  -- The form's actual submission date
    session_id,  -- Include the session ID
    -- For ROC Club - handle both old and new format field names
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
    year,  -- Year extracted from JSON with choice5 -> 2024 conversion
    term_start_date,  -- NULL for community safe space
    term_end_date,    -- NULL for community safe space
    form_filling_date,  -- The form's actual submission date
    session_id,  -- Include the session ID
    -- For Community Safe Space
    participant_data ->> 'full_name_first_middle_surname' AS participant_name,
    participant_data ->> 'gender' AS gender,  -- For Community Safe Space
    ward,
    county_code,
    constituency,
    assigned_to
FROM community_safe_space_participants
