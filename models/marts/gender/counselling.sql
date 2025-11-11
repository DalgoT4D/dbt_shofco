{{ config(
  materialized='table',
  tags=["commcare_extraction","gender_counselling", "gender"]
) }}

with
gender_counselling_data as (
    select distinct
        registered_by,
        initcap(
            replace(county, '_', ' ')
            ) as county,
        constituency,
        client_age,
        client_name,
        {{ validate_date("date_of_registration") }} as date_of_registration,
        client_gender,
        gender_site_code,
        client_marital_status,
        client_education_level,
        client_referenced_from,
        client_presenting_issues,
        case when client_presenting_issues like '%trauma_symptoms%' then 1 else 0 end as client_presenting_trauma_issues,
        case when client_presenting_issues like '%social_emotional_issues%' then 1 else 0 end as client_presenting_social_emotional_issues,
        case when client_presenting_issues like '%psychiatric_symptoms%' then 1 else 0 end as client_presenting_psychiatric_issues,
        case when client_presenting_issues like '%drug_abuse%' then 1 else 0 end as client_presenting_drug_abuse_issues,
        case when client_presenting_issues like '%behavioral_issues%' then 1 else 0 end as client_presenting_behavioral_issues,
        client_requires_followup_sessions,
        client_mental_health_score_behavioral_issues,
        client_mental_health_score_trauma_symptoms,
        client_mental_health_score_social_emotional_issues,
        client_mental_health_score_psychiatric_symptoms,
        client_mental_health_score_drug_abuse,
        reason_for_client_requiring_followup_sessions,
        is_client_being_referred_for_further_assistance,
        case_id
    from {{ ref("staging_gender_counselling_commcare") }}
)

select *
from gender_counselling_data
WHERE client_name <> 'test disappearance' AND client_name <> 't' AND LOWER(client_name) NOT LIKE '%test%'