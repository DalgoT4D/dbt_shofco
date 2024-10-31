{{ config(
  materialized='table'
) }}

WITH initial_assessments AS (
    -- Select and calculate the average score from the initial assessment
    SELECT
        case_id,
        initial_form_filling_date,
        (
            -- Calculate the average of all initial mental health scores
            (
                COALESCE(CAST(behavioral_issues AS NUMERIC), 0) +
                COALESCE(CAST(drug_abuse AS NUMERIC), 0) +
                COALESCE(CAST(psychiatric_symptoms AS NUMERIC), 0) +
                COALESCE(CAST(social_emotional_issues AS NUMERIC), 0) +
                COALESCE(CAST(trauma_symptoms AS NUMERIC), 0)
            ) / 
            NULLIF(
                (CASE WHEN behavioral_issues IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN drug_abuse IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN psychiatric_symptoms IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN social_emotional_issues IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN trauma_symptoms IS NOT NULL THEN 1 ELSE 0 END),
                0
            )
        ) AS initial_avg_score
    FROM {{ ref('staging_gender_initial_mental_health_assesment') }}
),

final_assessments AS (
    -- Select and calculate the average score from the final assessment
    SELECT
        case_id,
        final_form_filling_date,
        (
            -- Calculate the average of all final mental health scores
            (
                COALESCE(CAST(behavioral_issues_after_therapy AS NUMERIC), 0) +
                COALESCE(CAST(drug_abuse_after_therapy AS NUMERIC), 0) +
                COALESCE(CAST(psychiatric_symptoms_after_therapy AS NUMERIC), 0) +
                COALESCE(CAST(social_emotional_issues_after_therapy AS NUMERIC), 0) +
                COALESCE(CAST(trauma_symptoms_after_therapy AS NUMERIC), 0)
            ) / 
            NULLIF(
                (CASE WHEN behavioral_issues_after_therapy IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN drug_abuse_after_therapy IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN psychiatric_symptoms_after_therapy IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN social_emotional_issues_after_therapy IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN trauma_symptoms_after_therapy IS NOT NULL THEN 1 ELSE 0 END),
                0
            )
        ) AS final_avg_score
    FROM {{ ref('staging_gender_final_mental_health_assesment') }}
),

-- Join initial and final assessments on case_id
improved_scores AS (
    SELECT
        i.case_id,
        i.initial_avg_score,
        f.final_avg_score,
        i.initial_form_filling_date,
        f.final_form_filling_date,

        CASE 
            WHEN f.final_avg_score < i.initial_avg_score THEN 'Y'
            ELSE 'N'
        END AS improved
    FROM initial_assessments i
    JOIN final_assessments f
    ON i.case_id = f.case_id
)

-- Final output: case_id, initial average score, final average score, and whether the score improved
SELECT
    case_id,
    initial_avg_score,
    final_avg_score,
    improved,
    initial_form_filling_date,
    final_form_filling_date
FROM improved_scores
ORDER BY case_id