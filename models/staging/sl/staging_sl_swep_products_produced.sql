{{ config(
    materialized='incremental',
    unique_key='case_id',
    schema='staging_sl',
    alias='sl_swep_products_produced',
    tags=['commcare_extraction', 'sl_cases', 'sustainable_livelihoods', 'swep', 'production']
) }}

{% set properties_dict = {
    "owner_id": "owner_id",
    "mpesa_name": "mpesa_name",
    "mpesa_number": "mpesa_number",
    "product_made": "product_made",
    "amount_per_piece": "amount_per_piece",
    "number_of_pieces_produced": "number_of_pieces_produced",
    "date_of_production": "date_of_production",
    "revenue": "revenue"
} %}

{{ extract_case_table_from_sl_commcare_json('sl_swep_products_produced_case_data', properties_dict) }}
