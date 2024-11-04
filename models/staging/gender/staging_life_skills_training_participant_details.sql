{{ config(
  materialized='table'
) }}

WITH roc_club_participants AS (
    SELECT
        id,
        data::jsonb->'form'->>'target_group' AS target_group,
        (data::jsonb->'form'->'school_information'->>'term') AS term,  -- Extract term directly from JSON
        CASE 
            WHEN data::jsonb->'form'->'school_information'->>'year' = 'choice5' THEN '2024' 
            ELSE 'Unknown Year'
        END AS year,  -- Map 'choice5' to 2024
        data::jsonb->>'received_on' AS form_filling_date,  -- Use 'received_on' for the form-filling date
        data::jsonb->'form'->'meta'->>'instanceID' AS session_id,  -- Extract the session ID
        jsonb_array_elements(data->'form'->'membership_details') AS participant_data,

        -- Assign start and end dates for each term as timestamp
        CASE
            WHEN (data::jsonb->'form'->'school_information'->>'term') = 'term_1' THEN TO_TIMESTAMP('2024-08-31', 'YYYY-MM-DD') 
            WHEN (data::jsonb->'form'->'school_information'->>'term') = 'term_2' THEN TO_TIMESTAMP('2024-01-01', 'YYYY-MM-DD')
            WHEN (data::jsonb->'form'->'school_information'->>'term') = 'term3' THEN TO_TIMESTAMP('2024-04-21', 'YYYY-MM-DD')
            ELSE NULL::timestamp
        END AS term_start_date,
        
        CASE
            WHEN (data::jsonb->'form'->'school_information'->>'term') = 'term_1' THEN TO_TIMESTAMP('2024-12-31', 'YYYY-MM-DD') 
            WHEN (data::jsonb->'form'->'school_information'->>'term') = 'term_2' THEN TO_TIMESTAMP('2024-04-20', 'YYYY-MM-DD')
            WHEN (data::jsonb->'form'->'school_information'->>'term') = 'term3' THEN TO_TIMESTAMP('2024-08-31', 'YYYY-MM-DD')
            ELSE NULL::timestamp
        END AS term_end_date
    FROM {{ source('staging_gender', 'IIVC_Life_Skills_Training') }}
    WHERE data::jsonb->'form'->>'target_group' = 'roc_club'
    AND jsonb_typeof(data->'form'->'membership_details') = 'array'  -- Ensure it's an array
),
community_safe_space_participants AS (
    SELECT
        id,
        data::jsonb->'form'->>'target_group' AS target_group,
        'Unknown' AS term,  -- Set term as 'Unknown' for community safe space
        'Unknown' AS year,  -- Set year as 'Unknown' for community safe space
        data::jsonb->>'received_on' AS form_filling_date,  -- Use 'received_on' for the form-filling date
        data::jsonb->'form'->'meta'->>'instanceID' AS session_id,  -- Extract the session ID
        jsonb_array_elements(data->'form'->'community_safe_space_participants_details') AS participant_data,

        -- Use NULL for timestamp fields to maintain consistency with ROC Club
        NULL::timestamp AS term_start_date,
        NULL::timestamp AS term_end_date
    FROM {{ source('staging_gender', 'IIVC_Life_Skills_Training') }}
    WHERE data::jsonb->'form'->>'target_group' = 'community_safe_space'
    AND jsonb_typeof(data->'form'->'community_safe_space_participants_details') = 'array'  -- Ensure it's an array
)

-- Combine the participants from both roc_club and community_safe_space
SELECT
    id,
    target_group,
    term,
    year,  -- Year extracted from JSON or 'Unknown'
    term_start_date,  -- Start date based on the term or NULL
    term_end_date,    -- End date based on the term or NULL
    form_filling_date,  -- The form's actual submission date
    session_id,  -- Include the session ID
    participant_data->>'member_full_names_first_middle_surname' AS participant_name,  -- For ROC Club
    participant_data->>'member_gender' AS gender  -- For ROC Club
FROM roc_club_participants

UNION ALL

SELECT
    id,
    target_group,
    term,
    year,  -- Year set to 'Unknown'
    term_start_date,  -- NULL for community safe space
    term_end_date,    -- NULL for community safe space
    form_filling_date,  -- The form's actual submission date
    session_id,  -- Include the session ID
    participant_data->>'full_name_first_middle_surname' AS participant_name,  -- For Community Safe Space
    participant_data->>'gender' AS gender  -- For Community Safe Space
FROM community_safe_space_participants