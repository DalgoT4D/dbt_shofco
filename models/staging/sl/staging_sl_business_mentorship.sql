{{ 
    config(
        materialized='incremental',
        unique_key='case_id',
        alias='sl_business_mentorship',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'business_mentorship']
    ) 
}}

{% set properties_dict = {
    "owner_id": "owner_id",
    "external_id": "external_id",
    "mentor_name": "mentor_name",
    "date_of_visit": "date_of_visit",
    "funding_program": "funding_program",
    "type_of_business": "type_of_business",
    "mentorship_visit_number": "mentorship_visit_number",
    "what_solutions_did_you_identify_to_the_challenges_mentioned": "solutions_identified",
    "what_are_the_agreed_timelines_to_implement_each_of_the_solutions": "agreed_timelines",
    "what_are_the_main_challenges_the_mentee_faces_in_running_his_her_business": "main_challenges"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_business_mentorship_case_data', properties_dict) }}
