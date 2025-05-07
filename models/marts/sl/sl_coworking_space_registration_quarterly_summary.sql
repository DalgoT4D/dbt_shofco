{{ 
    config(
        materialized='table',
        alias='sl_coworking_space_registration_quarterly_summary',
        tags=['sl', 'coworking_space', 'registration', 'summary', 'quarterly']
    ) 
}}

WITH source AS (
    SELECT 
        *,
        EXTRACT(QUARTER FROM date_opened::timestamp) AS quarter_opened
    FROM {{ ref('sl_coworking_space_registration_analytics') }}
),

aggregated AS (
    SELECT
        year_opened,
        quarter_opened,
        CONCAT('Q', quarter_opened, '-', year_opened) AS quarter,
        COUNT(*) AS total_registrations,
        SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) AS female_count,
        SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) AS male_count,
        SUM(CASE WHEN is_plwd THEN 1 ELSE 0 END) AS plwd_count,
        SUM(CASE WHEN is_mother THEN 1 ELSE 0 END) AS mother_count,
        COUNT(DISTINCT owner_id) AS unique_officers
    FROM source
    GROUP BY year_opened, quarter_opened
)

SELECT * FROM aggregated
ORDER BY year_opened DESC, quarter_opened DESC
