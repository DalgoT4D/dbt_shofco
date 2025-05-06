{{ config(
    materialized='table',
    schema='intermediate_sl',
    alias='sl_business_grants_monthly_summary',
    tags=['sl', 'grants', 'monthly_summary']
) }}

WITH grants AS (
    SELECT * FROM {{ ref('sl_business_grants_analytics') }}
),

monthly_summary AS (
    SELECT
        year_allocated,
        month_allocated,
        COUNT(*) AS total_grants_issued,
        SUM(grant_amount) AS total_grant_amount,
        AVG(grant_amount) AS average_grant_amount,
        SUM(amount_requested) AS total_amount_requested,
        AVG(amount_requested) AS average_amount_requested
    FROM grants
    GROUP BY year_allocated, month_allocated
    ORDER BY year_allocated, month_allocated
)

SELECT * FROM monthly_summary
