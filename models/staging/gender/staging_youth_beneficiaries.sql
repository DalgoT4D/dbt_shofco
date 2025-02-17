{{ config(
  materialized='table', 
  tags=['gender_youth_beneficiaries', "gender"]
) }}

WITH initial_cases AS (
    SELECT
        data::json ->> 'case_id' AS case_id,
        data::json -> 'properties' ->> 'ward' AS ward,
        data::json -> 'properties' ->> 'county' AS county,
        data::json -> 'properties' ->> 'constituency' AS constituency,
        data::json
        -> 'properties'
        ->> 'date_of_registration' AS date_of_registration,
        data::json
        -> 'properties'
        ->> 'beneficiary_gender' AS beneficiary_gender,
        data::json
        -> 'properties'
        ->> 'name_of_the_beneficiary' AS name_of_the_beneficiary,
        data::json
        -> 'properties'
        ->> 'beneficiary_phone_number' AS beneficiary_phone_number,
        data::json -> 'properties' ->> 'registered_by' AS registered_by,
        data::json
        -> 'properties'
        ->> 'beneficiary_categories' AS beneficiary_categories,
        data::json -> 'properties' ->> 'case_name' AS case_name,
        data::json -> 'properties' ->> 'date_opened' AS date_opened,
        -- Placeholder for training_type, specific to subsequent cases
        NULL AS training_type,
        indexed_on,
        -- Placeholder for parent_case_id, specific to subsequent cases
        NULL AS parent_case_id
    FROM {{ source('staging_youth', 'zzz_case') }}
    WHERE data::json->'indices'->>'parent' IS NULL
    AND (data::jsonb->>'archived' IS NULL OR data::jsonb->>'archived' = 'false')
),

subsequent_cases AS (
    SELECT
        data::json ->> 'case_id' AS case_id,
        data::json -> 'properties' ->> 'ward' AS ward,
        data::json -> 'properties' ->> 'county' AS county,
        data::json -> 'properties' ->> 'constituency' AS constituency,
        data::json
        -> 'properties'
        ->> 'date_of_registration' AS date_of_registration,
        data::json
        -> 'properties'
        ->> 'beneficiary_gender' AS beneficiary_gender,
        data::json
        -> 'properties'
        ->> 'name_of_the_beneficiary' AS name_of_the_beneficiary,
        data::json
        -> 'properties'
        ->> 'beneficiary_phone_number' AS beneficiary_phone_number,
        data::json -> 'properties' ->> 'registered_by' AS registered_by,
        data::json
        -> 'properties'
        ->> 'beneficiary_categories' AS beneficiary_categories,
        data::json -> 'properties' ->> 'case_name' AS case_name,
        data::json -> 'properties' ->> 'date_opened' AS date_opened,
        -- Training type from subsequent cases
        data::json
        -> 'properties'
        ->> 'mental_health_training_session_type' AS training_type,
        indexed_on,
        -- Parent case ID for subsequent cases
        data::json -> 'indices' -> 'parent' ->> 'case_id' AS parent_case_id
    FROM {{ source('staging_youth', 'zzz_case') }}
    WHERE data::json -> 'indices' ->> 'parent' IS NOT NULL
),

final_cte as (
SELECT * FROM initial_cases
UNION ALL
SELECT * FROM subsequent_cases
)

{{ dbt_utils.deduplicate(
    relation='final_cte',
    partition_by='case_id',
    order_by='indexed_on desc',
   )
}}
