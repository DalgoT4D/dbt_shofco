{{ 
    config(
        materialized='incremental',
        unique_key='case_id',
        schema='staging_sl',
        alias='sl_coworking_space_registration',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'coworking_space_registration']
    ) 
}}

{% set properties_dict = {
    "owner_id": "owner_id",
    "external_id": "external_id",
    "gender": "gender",
    "is_plwd": "is_plwd",
    "id_number": "id_number",
    "is_mother": "is_mother",
    "first_name": "first_name",
    "middle_name": "middle_name",
    "last_name_surname": "last_name_surname",
    "nationality": "nationality",
    "officer_name": "officer_name",
    "date_of_birth": "date_of_birth",
    "primary_phone_number": "primary_phone_number",
    "alternative_phone_number": "alternative_phone_number"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_coworking_space_registration_case_data', properties_dict) }}
