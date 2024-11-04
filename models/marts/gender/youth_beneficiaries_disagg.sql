{{ config(
  materialized='table'
) }}

SELECT
    "case_id",
    "ward",
    "county",
    "constituency",
    "date_of_registration",
    "beneficiary_gender",
    "name_of_the_beneficiary",
    "beneficiary_phone_number",
    "registered_by",
    "case_name",
    "date_opened",
    "indexed_on",
    "beneficiary_categories",
    unnest(string_to_array("beneficiary_categories", ' ')) as "category"
FROM {{ ref('staging_youth_beneficiaries') }}
