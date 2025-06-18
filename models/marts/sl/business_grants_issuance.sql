{{ config(
  materialized='table',
  tags=["business_grants_issuance", "sl"]
) }}

SELECT
    case_id,
    user_id,
    date_modified::timestamp,
    
    full_name,
    unique_id,
    CASE WHEN age ~ '^[0-9]+$' THEN age::INT ELSE NULL END AS age,
    gender,
    ward,
    subcounty,
    county,

    grant_amount::NUMERIC,
    amount_requested::NUMERIC,
    grant_source,
    date_grant_allocated::date,

    mpesa_number,
    mpesa_first_name,
    mpesa_second_name,
    mpesa_full_name,

    any_business_grant,

    enumerator_full,
    enumerator_first,
    enumerator_last,

    facilitator_name,

    submitted_by,
    time_start::timestamp,
    time_end::timestamp,
    date_received::timestamp
FROM {{ ref("staging_sl_business_grants_issuance") }}