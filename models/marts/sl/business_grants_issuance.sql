{{ config(
  materialized='table',
  tags=["business_grants_issuance", "sl", "sl_marts"]
) }}

SELECT
    case_id,
    user_id,
    date_modified,
    
    pp_fullname AS full_name,
    pp_unique_id AS unique_id,
    CASE 
      WHEN age ~ '^[0-9]+$' THEN age::INT 
      ELSE NULL 
    END AS age,  -- Using coalesced field
    sex AS gender,  -- Using coalesced field
    pp_ahofco_ward AS ward,
    pp_shofco_subcounty AS subcounty,
    county AS county,

    CASE 
      WHEN grant_amount_bg ~ '^[0-9]+(\.[0-9]+)?$' THEN grant_amount_bg::NUMERIC
      ELSE NULL
    END AS grant_amount,
    CASE 
      WHEN amount_requested_bg ~ '^[0-9]+(\.[0-9]+)?$' THEN amount_requested_bg::NUMERIC
      ELSE NULL
    END AS amount_requested,
    grant_source_bg AS grant_source,
    {{ validate_date('date_grant_allocated_bg') }} AS date_grant_allocated,

    mpesa_number_bg AS mpesa_number,
    mpesa_first_name_bg AS mpesa_first_name,
    mpesa_second_name_bg AS mpesa_second_name,
    beneficiary_MPESA_full_name_bg AS mpesa_full_name,

    any_business_grant_bg AS any_business_grant,

    enumerator AS enumerator_full,
    enumerator_first_Name AS enumerator_first,  -- Using coalesced field
    enumerator_last_name AS enumerator_last,  -- Using coalesced field

    name_of_facilitator AS facilitator_name,

    user_id AS submitted_by,
    date_opened AS time_start,
    date_modified AS time_end,
    indexed_on AS date_received
FROM {{ ref("staging_sl_case_table") }}
WHERE any_business_grant_bg IS NOT NULL