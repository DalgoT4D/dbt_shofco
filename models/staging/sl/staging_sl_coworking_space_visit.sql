{{ 
    config(
        materialized='incremental',
        unique_key='case_id',
        schema='staging_sl',
        alias='sl_coworking_space_visit',
        tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'coworking_space']
    ) 
}}

{% set properties_dict = {
    "owner_id": "owner_id",
    "external_id": "external_id",
    "officer_name": "officer_name",
    "date_of_visit": "date_of_visit",
    "time_in": "time_in",
    "time_out": "time_out",
    "purpose_of_visit": "purpose_of_visit",
    "specific_reason_for_accessing_space": "specific_reason_for_accessing_space",
    "ward_shofco_site": "ward_shofco_site",
    "subcounty_shofco_site": "subcounty_shofco_site",
    "county_shofco_site": "county_shofco_site"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_coworking_space_visit_case_data', properties_dict) }}
