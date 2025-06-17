{{ config(
  materialized='table',
  tags=['business_grants_issuance', 'sl']
) }}

WITH issuance_data AS (
    SELECT
        id,

        -- Case metadata
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb -> 'form' -> 'case' ->> '@date_modified' AS date_modified,

        -- Personal details
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_fullname' AS full_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_unique_id' AS unique_id,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age' AS age,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_sex' AS gender,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_ahofco_ward' AS ward,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_subcounty' AS subcounty,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_county' AS county,

        -- Grant details
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'grant_amount_bg' AS grant_amount,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'amount_requested_bg' AS amount_requested,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'grant_source_bg' AS grant_source,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_grant_allocated_bg' AS date_grant_allocated,

        -- MPESA details
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'mpesa_number_bg' AS mpesa_number,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'mpesa_first_name_bg' AS mpesa_first_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'mpesa_second_name_bg' AS mpesa_second_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'beneficiary_MPESA_full-name_bg' AS mpesa_full_name,

        -- Grant received indicator
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'any_business_grant_bg' AS any_business_grant,

        -- Enumerator info
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator' AS enumerator_full,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerators_first' AS enumerator_first,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name' AS enumerator_last,

        -- Facilitator info
        data::jsonb -> 'form' -> 'facilitator_details' ->> 'name_of_facilitator' AS facilitator_name,

        -- Submission metadata
        data::jsonb -> 'form' -> 'meta' ->> 'username' AS submitted_by,
        data::jsonb -> 'form' -> 'meta' ->> 'timeStart' AS time_start,
        data::jsonb -> 'form' -> 'meta' ->> 'timeEnd' AS time_end,
        data::jsonb ->> 'received_on' AS date_received

    FROM {{ source('staging_sl', 'Business_Grants_Issuance_Form') }}
    WHERE data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
)

SELECT DISTINCT *
FROM issuance_data
