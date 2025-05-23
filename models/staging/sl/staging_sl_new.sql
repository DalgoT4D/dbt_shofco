{{ 
    config(
        materialized='incremental',
        unique_key='case_id',
        alias='sl_new',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'sl_new']
    ) 
}}

{% set properties_dict = {
    "pp_fullname": "pp_fullname",
    "pp_sex": "pp_sex",
    "pp_age": "pp_age",
    "contact_phone_number": "contact_phone_number",
    "date_of_birth_dir": "date_of_birth",
    "course_enrolled_sr": "course_enrolled",
    "employment_type_sr": "employment_type",
    "participants_program_sr": "participants_program",
    "willingness_to_relocate_sr": "willingness_to_relocate",
    "skills_before_joining_shofco_sr": "prior_skills",
    "training_activity_tc": "training_activity",
    "completed_training_tc": "completed_training",
    "confidence_to_find_employment_tc": "confident_to_find_employment",
    "support_to_help_succeed_as_entrepreneur_sr": "entrepreneur_support",
    "preferred_sectors_for_employment_sr": "preferred_employment_sector",
    "job_placement_support_needed_sr": "job_placement_support",
    "post_training_support_needed_sr": "post_training_support",
    "formal_education_dir": "formal_education",
    "level_of_education_dir": "education_level",
    "county_dir": "county",
    "constituency_dir": "constituency",
    "ward_dir": "ward",
    "date_of_registration_dir": "date_of_registration",
    "unique_id": "unique_id",
    "is_pwd": "is_pwd",
    "enumerator": "enumerator",
    "pp_shofco_county": "pp_shofco_county",
    "pp_shofco_subcounty": "pp_shofco_subcounty",
    "pp_ahofco_ward": "pp_ahofco_ward",
    "sex_dir": "sex_dir",
    "sex_uid": "sex_uid",
    "common_training_dir": "common_training",
    "unique_training_dir": "unique_training",
    "computer_literate_sr": "computer_literate",
    "interest_in_sales_work_ent": "interest_in_sales_work",
    "participants_type_of_work_sr": "participants_type_of_work",
    "status_of_education_dir": "status_of_education",
    "primary_phone_number_dir": "primary_phone_number",
    "alternative_phone_number_dir": "alternative_phone_number",
    "national_id_number_dir": "national_id_number",
    "is_existing_participant": "is_existing_participant",
    "specify_shofco_program_dir": "specify_shofco_program",
    "name_consent": "name_consent"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_new', properties_dict) }}
