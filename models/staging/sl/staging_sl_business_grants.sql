{{ config(
    materialized='incremental',
    unique_key='case_id',
    schema='staging_sl',
    alias='sl_business_grants',
    tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'grants']
) }}

{% set properties_dict = {
    "owner_id": "owner_id",
    "external_id": "external_id",
    "grant_amount": "grant_amount",
    "mpesa_number": "mpesa_number",
    "amount_requested": "amount_requested",
    "type_of_business": "type_of_business",
    "business_location": "business_location",
    "name_of_facilitator": "name_of_facilitator",
    "participant_program": "participant_program",
    "beneficiary_mpesa_name": "beneficiary_mpesa_name",
    "what_date_was_the_grant_allocated": "grant_allocated_date"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_business_grants_case_data', properties_dict) }}
