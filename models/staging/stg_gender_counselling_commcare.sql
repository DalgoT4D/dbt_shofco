{{
    config(
        materialized='incremental',
        unique_key='case_id',
        tags="commcare_extraction"
    )
}}


{% set commcare_case_type =  'gender_counselling_case_data' %}
{%

set case_type_properties_dict = {
    "county": "county",
    "constituency": "constituency",
    "client_age": "client_age",
    "date_of_registration": "date_of_registration",
    "client_gender": "client_gender",
    "gender_site_code":"gender_site_code",
    "client_marital_status": "client_marital_status",
    "client_education_level": "client_education_level",
    "client_referenced_from": "client_referenced_from",
    "client_presenting_issues": "client_presenting_issues",
    "client_requires_followup_sessions": "client_requires_followup_sessions",
    "client_mental_health_score_behavioral_issues": "client_mental_health_score_behavioral_issues",
    "is_client_being_referred_for_further_assistance": "is_client_being_referred_for_further_assistance",
    "client_mental_health_score_trauma_symptoms":"client_mental_health_score_trauma_symptoms",
    "client_mental_health_score_social_emotional_issues":"client_mental_health_score_social_emotional_issues",
    "client_mental_health_score_psychiatric_symptoms":"client_mental_health_score_psychiatric_symptoms",
    "client_mental_health_score_drug_abuse":"client_mental_health_score_drug_abuse",
    "reason_for_client_requiring_followup_sessions":"reason_for_client_requiring_followup_sessions"

}

-%}



{{ extract_case_table_from_commcare_json (commcare_case_type, case_type_properties_dict) }}