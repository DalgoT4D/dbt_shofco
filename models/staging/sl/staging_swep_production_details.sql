{{
    config(
        materialized="table", 
        unique_key="case_id", 
        tags=["commcare_extraction","swep_production_details", "sl"]
    )
}}


{% set commcare_case_type = "sl_swep_production_case_data" %}
{% set case_type_properties_dict = {
    "first_name": "first_name",
    "middle_name": "middle_name",
    "last_name": "last_name",
    "date_of_birth": "date_of_birth",
    "age": "age",
    "gender": "gender",
    "phone_number": "phone_number",
    "alternative_phone_number": "alternative_phone_number",
    "nationality": "nationality",
    "national_id_number": "national_id_number",
    "county": "county",
    "sub_county": "sub_county",
    "ward": "ward",
    "village": "village",
    "is_refugee": "is_refugee",
    "refugee_type": "refugee_type",
    "asylum_pass_number": "asylum_pass_number",
    "nationality_if_other": "nationality_if_other",
    "non_citizen_nationality": "non_citizen_nationality",
    "non_citizen_id_number": "non_citizen_id_number",
    "registration_document": "registration_document",
    "external_id": "external_id",
    "referral": "referral",
    "who_referred_the_participant": "who_referred_the_participant",
    "is_plwd": "is_plwd",
    "is_mother": "is_mother",
    "baby_below_four_years": "baby_below_four_years",
    "date_of_registration": "date_of_registration",
    "level_of_education": "level_of_education",
    "county_shofco_site": "county_shofco_site",
    "subcounty_shofco_site": "subcounty_shofco_site",
    "ward_shofco_site": "ward_shofco_site",
    "owner_id": "owner_id",
} -%}


with swep_production_details_cte as ({{
    extract_case_table_from_sl_commcare_json(
        commcare_case_type, case_type_properties_dict
    )
}})

{{ dbt_utils.deduplicate(
    relation='swep_production_details_cte',
    partition_by='case_id',
    order_by='indexed_on desc',
   )
}}
