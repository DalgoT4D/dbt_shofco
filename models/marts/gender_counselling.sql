with
    gender_counselling_data as (
        select distinct
            county,
            constituency,
            client_age,
            {{ validate_date("date_of_registration") }} as date_of_registration,
            client_gender,
            gender_site_code,
            client_marital_status,
            client_education_level,
            client_referenced_from,
            client_presenting_issues,
            client_requires_followup_sessions,
            client_mental_health_score_behavioral_issues,
            client_mental_health_score_trauma_symptoms,
            client_mental_health_score_social_emotional_issues,
            client_mental_health_score_psychiatric_symptoms,
            client_mental_health_score_drug_abuse,
            reason_for_client_requiring_followup_sessions,
            is_client_being_referred_for_further_assistance,
            case_id
        from {{ ref("stg_gender_counselling_commcare") }}
    )
select *
from gender_counselling_data
