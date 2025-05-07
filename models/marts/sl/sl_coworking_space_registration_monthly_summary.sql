{{ 
    config(
        materialized='table',
        schema='dev_sl',
        alias='sl_coworking_space_registration_monthly_summary',
        tags=['sl', 'coworking_space', 'registration', 'summary', 'monthly']
    ) 
}}

WITH source AS (
    SELECT * FROM {{ ref('sl_coworking_space_registration_analytics') }}
),

aggregated AS (
    SELECT
        year_opened,
        month_opened,
        CONCAT(year_opened, '-', LPAD(month_opened::text, 2, '0')) AS month,
        COUNT(*) AS total_registrations,
        SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) AS female_count,
        SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) AS male_count,
        SUM(CASE WHEN is_plwd THEN 1 ELSE 0 END) AS plwd_count,
        SUM(CASE WHEN is_mother THEN 1 ELSE 0 END) AS mother_count,
        COUNT(DISTINCT owner_id) AS unique_officers
    FROM source
    GROUP BY year_opened, month_opened
)

SELECT * FROM aggregated
ORDER BY year_opened DESC, month_opened DESC
