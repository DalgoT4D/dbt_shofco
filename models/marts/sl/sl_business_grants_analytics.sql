{{ config(
    materialized='table',
    alias='sl_business_grants_analytics',
    tags=['sl', 'grants', 'analytics']
) }}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_business_grants') }}
),

processed AS (
    SELECT
        case_id,
        case_name,
        date_opened,
        grant_allocated_date,
        name_of_facilitator,
        participant_program,
        type_of_business,
        business_location,
        mpesa_number,
        beneficiary_mpesa_name,
        CAST(NULLIF(grant_amount, '') AS INTEGER) AS grant_amount,
        CAST(NULLIF(amount_requested, '') AS INTEGER) AS amount_requested,
        EXTRACT(YEAR FROM grant_allocated_date::date) AS year_allocated,
        EXTRACT(MONTH FROM grant_allocated_date::date) AS month_allocated
    FROM source
)

SELECT * FROM processed
