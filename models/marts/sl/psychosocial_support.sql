{{ config(
  materialized='table',
  tags=['sl', 'psychosocial']
) }}

WITH psycho_data AS (
    SELECT
        id,
        case_id,
        user_id,

        -- Safely cast received_on to date
        CASE 
            WHEN received_on ~ '^\d{4}-\d{2}-\d{2}' THEN received_on::DATE
            ELSE NULL
        END AS received_on,

        initial_processing_complete,

        -- Safely cast meeting_date to date
        CASE 
            WHEN meeting_date ~ '^\d{4}-\d{2}-\d{2}' THEN meeting_date::DATE
            ELSE NULL
        END AS meeting_date,

        meeting_topic,
        group_name,
        enumerator_full_name,
        enumerator_first,
        enumerator_last,

        -- Fix: Ensure regex is applied to text, not integer
        CASE 
            WHEN people_attending::text ~ '^\d+$' THEN people_attending::INT
            ELSE NULL
        END AS people_attending,

        facilitator_name,
        facilitator_profession,
        ward,
        subcounty,
        county,
        username
    FROM {{ ref('staging_psychosocial_register') }}
)

SELECT
    *,
    CASE
        WHEN LOWER(group_name) LIKE '%swep%' THEN TRUE
        ELSE FALSE
    END AS is_swep_session
FROM psycho_data
