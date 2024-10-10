WITH mental_health_assessment AS (
    -- Reference the original model (assuming it's named 'mental_health_assessment_data')
    SELECT
        case_id,
        sessions_attended,
        concluding_comments,
        date_of_final_assessment,
        session_id
    FROM {{ ref('stg_gender_final_mental_health_assesment') }}  -- This references the base model
)
SELECT
    case_id,
    sessions_attended,
    concluding_comments,
    date_of_final_assessment,
    session_id
FROM mental_health_assessment
WHERE sessions_attended IS NOT NULL  -- Filter to ensure there are valid session counts