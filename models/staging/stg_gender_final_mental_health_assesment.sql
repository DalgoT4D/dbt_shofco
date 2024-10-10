WITH mental_health_assessment_data AS (
    SELECT
        id,
        -- Correctly extract the case ID from 'case'
        data::jsonb->'form'->'case'->>'@case_id' AS case_id,  -- Extract the case ID
        -- Correctly extract the user ID from 'case'
        data::jsonb->'form'->'case'->>'@user_id' AS user_id,  -- Extract the user ID
        data::jsonb->'form'->>'sessions_attended' AS sessions_attended,
        data::jsonb->'form'->>'concluding_comments' AS concluding_comments,
        data::jsonb->'form'->>'final_assessment_by' AS final_assessment_by,
        data::jsonb->'form'->>'date_of_final_assessment' AS date_of_final_assessment,
        data::jsonb->'form'->>'please_provide_more_information_about_the_clients_satisfaction_score' AS client_satisfaction_score_comments,
        data::jsonb->'form'->>'what_is_the_clients_level_of_satisfaction_with_the_counselling_services_pro' AS client_satisfaction_level,

        -- Extract all relevant mental health scores after therapy from both 'update' and 'client_mental_health_scores'
        COALESCE(
            data::jsonb->'form'->'update'->>'client_mental_health_score_behavioral_issues_after_therapy',
            data::jsonb->'form'->'client_mental_health_scores'->>'client_mental_health_score_behavioral_issues_after_therapy'
        ) AS behavioral_issues_after_therapy,

        COALESCE(
            data::jsonb->'form'->'update'->>'client_mental_health_score_drug_abuse_after_therapy',
            data::jsonb->'form'->'client_mental_health_scores'->>'client_mental_health_score_drug_abuse_after_therapy'
        ) AS drug_abuse_after_therapy,

        COALESCE(
            data::jsonb->'form'->'update'->>'client_mental_health_score_psychiatric_symptoms_after_therapy',
            data::jsonb->'form'->'client_mental_health_scores'->>'client_mental_health_score_psychiatric_symptoms_after_therapy'
        ) AS psychiatric_symptoms_after_therapy,

        COALESCE(
            data::jsonb->'form'->'update'->>'client_mental_health_score_social_emotional_issues_after_therapy',
            data::jsonb->'form'->'client_mental_health_scores'->>'client_mental_health_score_social_emotional_issues_after_therapy'
        ) AS social_emotional_issues_after_therapy,

        COALESCE(
            data::jsonb->'form'->'update'->>'client_mental_health_score_trauma_symptoms_after_therapy',
            data::jsonb->'form'->'client_mental_health_scores'->>'client_mental_health_score_trauma_symptoms_after_therapy'
        ) AS trauma_symptoms_after_therapy,

        data::jsonb->'meta'->>'instanceID' AS session_id,  -- Extract the session ID
        data::jsonb->>'received_on' AS form_filling_date  -- Use 'received_on' for the form-filling date
    FROM t4d_staging."Final_Mental_Health_Assessment_Form"
)
SELECT
    id,
    case_id,  -- Include case ID
    user_id,  -- Include user ID
    sessions_attended,
    concluding_comments,
    final_assessment_by,
    date_of_final_assessment,

    -- Include all relevant mental health scores after therapy
    behavioral_issues_after_therapy,
    drug_abuse_after_therapy,
    psychiatric_symptoms_after_therapy,
    social_emotional_issues_after_therapy,
    trauma_symptoms_after_therapy,

    client_satisfaction_score_comments,
    client_satisfaction_level,
    session_id,
    form_filling_date
FROM mental_health_assessment_data