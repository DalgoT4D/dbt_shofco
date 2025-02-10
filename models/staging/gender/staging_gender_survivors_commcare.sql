{{
    config(
        materialized="table",
        unique_key="case_id",
        alias="staging_gender_survivors_commcare",
        tags= ["commcare_extraction", "gender_survivors", "gender"]
    )
}}


{% set commcare_case_type = "survivors_case_data" %}
{% set case_type_properties_dict = {
    "assigned_to": "assigned_to",
    "county": "county",
    "constituency": "constituency",
    "date_of_birth": "date_of_birth",
    "date_of_registration": "date_of_registration",
    "gender": "gender",
    "gender_site_code_of_registration": "gender_site_code_of_registration",
    "police_ob_number_for_registration": "police_ob_number_for_registration",
    "registered_by": "registered_by",
    "village": "village",
    "ward": "ward",
    "what_is_the_age_provided": "what_is_the_age_provided",
    "what_is_the_year_of_birth_of_survivor": "what_is_the_year_of_birth_of_survivor",
} -%}


with survivors_cte as ({{
    extract_case_table_from_gender_commcare_json(
        commcare_case_type, case_type_properties_dict
    )
}})

-- with cleaned_survivors_cte as (
--     SELECT 
--         "case_id",
--         "assigned_to",
--         "county",
--         "constituency",
--         "date_of_birth",
--         NULLIF("date_of_registration", '')::date,
--         "gender",
--         "gender_site_code_of_registration",
--         "police_ob_number_for_registration",
--         "registered_by",
--         "village",
--         "ward",
--         "what_is_the_age_provided",
--         "what_is_the_year_of_birth_of_survivor"
-- )

{{ dbt_utils.deduplicate(
    relation='survivors_cte',
    partition_by='case_id',
    order_by='indexed_on desc',
   )
}}
