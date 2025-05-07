{{
    config(
        materialized='incremental',
        unique_key='case_id',
        alias='sl_swep_production',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'swep']
    )
}}

{% set properties_dict = {
    "owner_id": "owner_id",
    "external_id": "external_id",
    "first_name": "first_name",
    "middle_name": "middle_name",
    "last_name": "last_name",
    "gender": "gender",
    "age": "age",
    "date_of_birth": "date_of_birth",
    "national_id_number": "national_id_number",
    "non_citizen_id_number": "non_citizen_id_number",
    "registration_document": "registration_document",
    "nationality": "nationality",
    "nationality_if_other": "nationality_if_other",
    "non_citizen_nationality": "non_citizen_nationality",
    "is_refugee": "is_refugee",
    "refugee_type": "refugee_type",
    "asylum_pass_number": "asylum_pass_number",
    "phone_number": "phone_number",
    "alternative_phone_number": "alt_phone_number",
    "level_of_education": "education_level",
    "is_mother": "is_mother",
    "baby_below_four_years": "baby_below_four_years",
    "referral": "referral",
    "who_referred_the_participant": "referral_source",
    "village": "village",
    "ward": "ward",
    "sub_county": "sub_county",
    "county": "county",
    "ward_shofco_site": "shofco_ward",
    "subcounty_shofco_site": "shofco_subcounty",
    "county_shofco_site": "shofco_county",
    "date_of_registration": "registration_date"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_swep_production_case_data', properties_dict) }}
