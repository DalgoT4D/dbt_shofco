{{ config(
  materialized='table',
  tags=["gender_mental_health_assesment", "gender"]
) }}

WITH initial_mental_health_assessment_staging AS (
    SELECT
        id,  -- Extract the unique form ID

        -- Extract the case ID and user ID from 'case'
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,

        -- Extract all relevant mental health scores from 'client_mental_health_scores'
        data::jsonb
        -> 'form'
        -> 'client_mental_health_scores'
        ->> 'client_mental_health_score_behavioral_issues' AS behavioral_issues,
        data::jsonb
        -> 'form'
        -> 'client_mental_health_scores'
        ->> 'client_mental_health_score_drug_abuse' AS drug_abuse,
        data::jsonb
        -> 'form'
        -> 'client_mental_health_scores'
        ->> 'client_mental_health_score_psychiatric_symptoms' AS psychiatric_symptoms,
        data::jsonb
        -> 'form'
        -> 'client_mental_health_scores'
        ->> 'client_mental_health_score_social_emotional_issues' AS social_emotional_issues,
        data::jsonb
        -> 'form'
        -> 'client_mental_health_scores'
        ->> 'client_mental_health_score_trauma_symptoms' AS trauma_symptoms,

        -- Extract metadata like session ID and form filling date
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS session_id,
        data::jsonb ->> 'received_on' AS initial_form_filling_date
    FROM {{ source('staging_gender', 'Initial_Mental_Health_Asssessment') }}
    WHERE
        data::jsonb ->> 'archived' IS NULL
        OR data::jsonb ->> 'archived' = 'false'
)

SELECT DISTINCT
    id,
    initial_form_filling_date,
    case_id,
    user_id,
    -- Include all relevant mental health scores as separate columns
    behavioral_issues,
    drug_abuse,
    psychiatric_symptoms,
    social_emotional_issues,
    trauma_symptoms,
    session_id
FROM initial_mental_health_assessment_staging
