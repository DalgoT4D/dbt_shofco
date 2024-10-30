SELECT
    case_id,
    sessions_attended,
    concluding_comments,
    date_of_final_assessment,
    session_id
FROM {{ ref('staging_gender_final_mental_health_assesment') }}
WHERE sessions_attended IS NOT NULL  -- Filter to ensure there are valid session counts