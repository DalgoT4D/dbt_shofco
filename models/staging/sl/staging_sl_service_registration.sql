{{
    config(
        materialized='incremental',
        unique_key='case_id',
        schema='staging_sl',
        alias='sl_service_registration',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'service_registration']
    )
}}

{% set properties_dict = {
    "owner_id": "owner_id",
    "gender": "gender",
    "is_plwd": "is_plwd",
    "is_mother": "is_mother",
    "is_refugee": "is_refugee",
    "referral": "referral",
    "officer_name": "officer_name",
    "phone_number": "phone_number",
    "nationality": "nationality",
    "alien_card_number": "alien_card_number",
    "asylum_pass_number": "asylum_pass_number",
    "non_citizen_nationality": "non_citizen_nationality",
    "national_id_number": "national_id_number",
    "level_of_education": "education_level",
    "date_of_birth": "date_of_birth",
    "date_of_registration": "registration_date",
    "start_date": "start_date",
    "completion_date": "completion_date",
    "completed_training": "completed_training",
    "training_course": "training_course",
    "training_activity": "training_activity",
    "training_activity_completed": "training_activity_completed",
    "service_of_interest": "service_of_interest",
    "activity_of_interest": "activity_of_interest",
    "participant_program": "participant_program",
    "post_training_support": "post_training_support",
    "daycare_support_needed": "daycare_support_needed",
    "placement_opportunities": "placement_opportunities",
    "is_the_participant_employed": "is_employed",
    "business_support_of_interest": "business_support",
    "adavnced_it_course_of_interest": "advanced_it_course",
    "vocational_courses_of_interest": "vocational_courses",
    "own_business": "own_business",
    "name_of_facilitator": "facilitator_name",
    "who_referred_the_participant": "referral_source",
    "if_shofco_programs_specify_the_program": "shofco_program",
    "ward_shofco_site": "shofco_ward",
    "subcounty_shofco_site": "shofco_subcounty",
    "county_shofco_site": "shofco_county"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_service_registration_case_data', properties_dict) }}
