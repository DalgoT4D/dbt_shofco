{{
    config(
        materialized='incremental',
        unique_key='case_id',
        alias='stg_gender_safe_house_commcare',
        tags="commcare_extraction"
    )
}}


{% set commcare_case_type =  'safe_house_case_data' %}
{%

set case_type_properties_dict = {
    "any_known_existing_medical_conditions": "any_known_existing_medical_conditions",
    "any_medical_condition_requiring_special_dietray_provision": "any_medical_condition_requiring_special_dietray_provision",
    "client_addictive_substances": "client_addictive_substances",
    "client_dietary_provisions": "client_dietary_provisions",
    "client_medical_conditions": "client_medical_conditions",
    "client_medications": "client_medications",
    "date_and_time_of_person_receiving_discharged_survivor_signature": "date_and_time_of_person_receiving_discharged_survivor_signature",
    "date_of_discharge": "date_of_discharge",
    "date_of_safehouse_onboarding": "date_of_safehouse_onboarding",
    "designation_of_person_receiving_discharged_survivor": "designation_of_person_receiving_discharged_survivor",
    "discharge_approval_by_gender_director": "discharge_approval_by_gender_director",
    "discharge_case_worker_date_of_approval": "discharge_case_worker_date_of_approval",
    "duration_of_extended_stay_in_safe_house_in_hours": "duration_of_extended_stay_in_safe_house_in_hours",
    "expected_next_set_of_actions": "expected_next_set_of_actions",
    "is_the_client_on_any_medication": "is_the_client_on_any_medication",
    "is_the_client_using_any_any_addictive_substance_whether_controlled_or_not": "is_the_client_using_any_any_addictive_substance_whether_controlled_or_not",
    "justification_for_extended_stay_in_safehouse": "justification_for_extended_stay_in_safehouse",
    "key_actions_taken_while_the_client_was_in_the_safe_house": "key_actions_taken_while_the_client_was_in_the_safe_house",
    "last_amended_at": "last_amended_at",
    "mobile_number_of_person_receiving_discharged_survivor": "mobile_number_of_person_receiving_discharged_survivor",
    "other_safehouse_onboarding_referrral": "other_safehouse_onboarding_referrral",
    "presenting_sgbv_issues,": "presenting_sgbv_issues",
    "reasons_for_consideration_of_admission": "reasons_for_consideration_of_admission",
    "safehouse_onboarding_referral": "safehouse_onboarding_referral",
    "gender_director_date_of_determination_of_discharge": "gender_director_date_of_determination_of_discharge",
    "gender_director_date_of_determination_of_onboarding": "gender_director_date_of_determination_of_onboarding",
    "medical_conditions_requiring_special_dietray_provision": "medical_conditions_requiring_special_dietray_provision",
    "onboarding_approval_by_gender_director": "onboarding_approval_by_gender_director",
    "onboarding_case_worker_date_of_approval": "onboarding_case_worker_date_of_approval",
    "reason_for_denial_of_discharge_by_gender_director": "reason_for_denial_of_discharge_by_gender_director",
    "reason_for_denial_of_onboarding_by_gender_director": "reason_for_denial_of_onboarding_by_gender_director"
}

-%}



{{ extract_case_table_from_commcare_json (commcare_case_type, case_type_properties_dict, true) }}