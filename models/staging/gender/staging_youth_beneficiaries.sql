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
        NULL AS training_type,
        indexed_on,
        NULL AS parent_case_id
    FROM {{ source('staging_youth', 'zzz_case') }}
    WHERE
        data::json->'indices'->>'parent' IS NULL
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
        data::json
        -> 'properties'
        ->> 'mental_health_training_session_type' AS training_type,
        indexed_on,
        data::json -> 'indices' -> 'parent' ->> 'case_id' AS parent_case_id
    FROM {{ source('staging_youth', 'zzz_case') }}
    WHERE data::json -> 'indices' ->> 'parent' IS NOT NULL
),

final_cte AS (
    SELECT * FROM initial_cases
    UNION ALL
    SELECT * FROM subsequent_cases
),

with_county_name AS (
    SELECT
        f.case_id,
        f.ward,
        COALESCE(wl.county_name, f.county) AS county,
        f.constituency,
        f.date_of_registration,
        f.beneficiary_gender,
        f.name_of_the_beneficiary,
        f.beneficiary_phone_number,
        f.registered_by,
        f.beneficiary_categories,
        f.case_name,
        f.date_opened,
        f.training_type,
        f.indexed_on,
        f.parent_case_id
    FROM final_cte f
    LEFT JOIN {{ source('staging_gender', 'ward_lookup') }} wl
        ON UPPER(f.county) = UPPER(wl.county_code)
)

{{ dbt_utils.deduplicate(
    relation='with_county_name',
    partition_by='case_id',
    order_by='indexed_on desc',
   )
}}