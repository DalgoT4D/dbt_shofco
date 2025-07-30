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

        CASE 
            WHEN (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age') ~ '^[0-9]+$' 
            THEN (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age')::int 
            ELSE NULL 
        END AS age,

        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_sex' AS gender,

        -- Locations
        NULLIF(COALESCE(
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_ahofco_ward',
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'ward'
        ), '') AS ward,

        NULLIF(COALESCE(
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_subcounty',
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'sub_county'
        ), '') AS subcounty,

        NULLIF(COALESCE(
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_county',
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'county'
        ), '') AS county,

        -- Grant details
        CASE 
            WHEN (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'grant_amount_bg') ~ '^[0-9]+(\.[0-9]+)?$' 
            THEN (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'grant_amount_bg')::numeric 
            ELSE NULL 
        END AS grant_amount,

        CASE 
            WHEN (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'amount_requested_bg') ~ '^[0-9]+(\.[0-9]+)?$' 
            THEN (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'amount_requested_bg')::numeric 
            ELSE NULL 
        END AS amount_requested,

        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'grant_source_bg' AS grant_source,

        CASE 
            WHEN (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_grant_allocated_bg') ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_grant_allocated_bg')::date 
            ELSE NULL 
        END AS date_grant_allocated,

        -- MPESA details
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'mpesa_number_bg' AS mpesa_number,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'mpesa_first_name_bg' AS mpesa_first_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'mpesa_second_name_bg' AS mpesa_second_name,

        NULLIF(COALESCE(
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'beneficiary_MPESA_full-name_bg',
            TRIM(
                COALESCE(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'mpesa_first_name_bg', '') || ' ' ||
                COALESCE(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'mpesa_second_name_bg', '')
            )
        ), '') AS mpesa_full_name,

        -- Grant received indicator
        LOWER(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'any_business_grant_bg') AS any_business_grant,

        -- Enumerator info
        NULLIF(COALESCE(
            data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator',
            TRIM(
                COALESCE(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerators_first', '') || ' ' ||
                COALESCE(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name', '')
            )
        ), '') AS enumerator_full,

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
