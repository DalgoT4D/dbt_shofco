{{ 
    config(
        materialized='incremental',
        unique_key='case_id',
        schema='staging_sl',
        alias='sl_post_training',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'post_training']
    ) 
}}

{% set properties_dict = {
    "owner_id": "owner_id",
    "external_id": "external_id",
    "officer_name": "officer_name",
    "job_placement": "job_placement",
    "program_donor": "program_donor",
    "employment_type": "employment_type",
    "training_activity": "training_activity",
    "internship_placement": "internship_placement",
    "what_is_the_position": "position",
    "is_placement_within_SHOFCO": "placement_within_shofco",
    "apprenticeship_opportunities": "apprenticeship_opportunities",
    "name_of_organization_for_placement": "placement_organization"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_post_training_case_data', properties_dict) }}
