WITH roc_club_participants AS (
    SELECT
        id,
        data::jsonb->'form'->>'target_group' AS target_group,
        data::jsonb->>'received_on' AS form_filling_date,  -- Use 'received_on' for the form-filling date
        data::jsonb->'form'->'meta'->>'instanceID' AS session_id,  -- Extract the session ID
        jsonb_array_elements(data->'form'->'membership_details') AS participant_data
    FROM {{ source('source_commcare', 'IIVC_Life_Skills_Training') }}
    WHERE data::jsonb->'form'->>'target_group' = 'roc_club'
    AND jsonb_typeof(data->'form'->'membership_details') = 'array'  -- Ensure it's an array
),
community_safe_space_participants AS (
    SELECT
        id,
        data::jsonb->'form'->>'target_group' AS target_group,
        data::jsonb->>'received_on' AS form_filling_date,  -- Use 'received_on' for the form-filling date
        data::jsonb->'form'->'meta'->>'instanceID' AS session_id,  -- Extract the session ID
        jsonb_array_elements(data->'form'->'community_safe_space_participants_details') AS participant_data
    FROM {{ source('source_commcare', 'IIVC_Life_Skills_Training') }}
    WHERE data::jsonb->'form'->>'target_group' = 'community_safe_space'
    AND jsonb_typeof(data->'form'->'community_safe_space_participants_details') = 'array'  -- Ensure it's an array
)
-- Combine the participants from both roc_club and community_safe_space
SELECT
    id,
    target_group,
    form_filling_date,  -- Use the form filling date from 'received_on'
    session_id,  -- Include the session ID
    participant_data->>'member_full_names_first_middle_surname' AS participant_name,  -- For ROC Club
    participant_data->>'member_gender' AS gender  -- For ROC Club
FROM roc_club_participants

UNION ALL

SELECT
    id,
    target_group,
    form_filling_date,  -- Use the form filling date from 'received_on'
    session_id,  -- Include the session ID
    participant_data->>'full_name_first_middle_surname' AS participant_name,  -- For Community Safe Space
    participant_data->>'gender' AS gender  -- For Community Safe Space
FROM community_safe_space_participants