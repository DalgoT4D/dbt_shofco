{{
    config(
        materialized='incremental',
        unique_key='case_id',
        alias='sl_mobilization',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'mobilization']
    )
}}

{% set properties_dict = {
    "ward": "ward",
    "county": "county",
    "gender": "gender",
    "is_plwd": "is_plwd",
    "village": "village",
    "fullname": "fullname",
    "referral": "referral",
    "first_name": "first_name",
    "is_refugee": "is_refugee",
    "sub_county": "sub_county",
    "middle_name": "middle_name",
    "phone_number": "phone_number",
    "date_of_birth": "date_of_birth",
    "registered_by": "registered_by",
    "training_course": "training_course",
    "ward_shofco_site": "ward_shofco_site",
    "last_name_surname": "last_name_surname",
    "county_shofco_site": "county_shofco_site",
    "level_of_education": "level_of_education",
    "national_id_number": "national_id_number",
    "additional_comments": "additional_comments",
    "service_of_interest": "service_of_interest",
    "date_of_registration": "date_of_registration",
    "subcounty_shofco_site": "subcounty_shofco_site",
    "tvet_course_of_interest": "tvet_course_of_interest",
    "alternative_phone_number": "alternative_phone_number",
    "who_referred_the_participant": "who_referred_the_participant",
    "vocational_courses_of_interest": "vocational_courses_of_interest"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_mobilization_case_data', properties_dict) }}
