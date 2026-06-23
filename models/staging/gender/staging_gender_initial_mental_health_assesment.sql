{{ config(
  materialized='table',
  tags=["gender_mental_health_assesment", "gender"]
) }}

WITH initial_mental_health_assessment_staging AS (
    SELECT
        id,

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

        -- Extract raw county for normalization
        data::jsonb -> 'form' -> 'geographical_location_of_counselling' ->> 'county' AS county_raw,
        data::jsonb -> 'form' -> 'geographical_location_of_counselling' ->> 'village' AS village,
        data::jsonb -> 'form' -> 'geographical_location_of_counselling' ->> 'gender_site_code' AS gender_site_code,

        -- Extract metadata like session ID and form filling date
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS session_id,
        data::jsonb ->> 'received_on' AS initial_form_filling_date
    FROM {{ source('staging_gender', 'Initial_Mental_Health_Asssessment') }}
    WHERE
        data::jsonb ->> 'archived' IS NULL
        OR data::jsonb ->> 'archived' = 'false'
)

SELECT DISTINCT
    s.id,
    s.initial_form_filling_date,
    s.case_id,
    s.user_id,
    s.behavioral_issues,
    s.drug_abuse,
    s.psychiatric_symptoms,
    s.social_emotional_issues,
    s.trauma_symptoms,
    -- Normalize county: handle short codes, underscores, hyphens, mixed case
    CASE
        WHEN LENGTH(TRIM(s.county_raw)) <= 3
            THEN wl.county_name
        ELSE INITCAP(REPLACE(REPLACE(TRIM(s.county_raw), '_', ' '), '-', ' '))
    END as county,
    s.village,
    s.gender_site_code,
    s.session_id
FROM initial_mental_health_assessment_staging s
LEFT JOIN {{ source('staging_gender', 'ward_lookup') }} wl
    ON UPPER(TRIM(s.county_raw)) = UPPER(wl.county_code)