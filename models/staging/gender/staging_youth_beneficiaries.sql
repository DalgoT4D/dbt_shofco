{{ config(
  materialized='table', tags='gender_youth_beneficiaries'
) }}

WITH initial_cases AS (
    SELECT
        data::json->>'case_id' AS case_id,
        data::json->'properties'->>'ward' AS ward,
        data::json->'properties'->>'county' AS county,
        data::json->'properties'->>'constituency' AS constituency,
        data::json->'properties'->>'date_of_registration' AS date_of_registration,
        data::json->'properties'->>'beneficiary_gender' AS beneficiary_gender,
        data::json->'properties'->>'name_of_the_beneficiary' AS name_of_the_beneficiary,
        data::json->'properties'->>'beneficiary_phone_number' AS beneficiary_phone_number,
        data::json->'properties'->>'registered_by' AS registered_by,
        data::json->'properties'->>'beneficiary_categories' AS beneficiary_categories,
        data::json->'properties'->>'case_name' AS case_name,
        data::json->'properties'->>'date_opened' AS date_opened,
        NULL AS training_type,  -- Placeholder for training_type, specific to subsequent cases
        indexed_on,
        NULL AS parent_case_id  -- Placeholder for parent_case_id, specific to subsequent cases
    FROM {{ source('staging_youth', 'zzz_case') }}
    WHERE data::json->'indices'->>'parent' IS NULL
),

subsequent_cases AS (
    SELECT
        data::json->>'case_id' AS case_id,
        data::json->'properties'->>'ward' AS ward,
        data::json->'properties'->>'county' AS county,
        data::json->'properties'->>'constituency' AS constituency,
        data::json->'properties'->>'date_of_registration' AS date_of_registration,
        data::json->'properties'->>'beneficiary_gender' AS beneficiary_gender,
        data::json->'properties'->>'name_of_the_beneficiary' AS name_of_the_beneficiary,
        data::json->'properties'->>'beneficiary_phone_number' AS beneficiary_phone_number,
        data::json->'properties'->>'registered_by' AS registered_by,
        data::json->'properties'->>'beneficiary_categories' AS beneficiary_categories,
        data::json->'properties'->>'case_name' AS case_name,
        data::json->'properties'->>'date_opened' AS date_opened,
        data::json->'properties'->>'mental_health_training_session_type' AS training_type,  -- Training type from subsequent cases
        indexed_on,
        data::json->'indices'->'parent'->>'case_id' AS parent_case_id  -- Parent case ID for subsequent cases
    FROM {{ source('staging_youth', 'zzz_case') }}
    WHERE data::json->'indices'->>'parent' IS NOT NULL
)

SELECT * FROM initial_cases
UNION ALL
SELECT * FROM subsequent_cases