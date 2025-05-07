{{ 
    config(
        materialized='table',
        schema='marts_sl',
        alias='sl_coworking_space_visit_quarterly_summary',
        tags=['sl', 'coworking_space', 'quarterly_summary']
    ) 
}}

WITH source AS (
    SELECT * FROM {{ ref('sl_coworking_space_visit_analytics') }}
),

aggregated AS (
    SELECT
        year_of_visit,
        quarter_of_visit,
        county_shofco_site,
        subcounty_shofco_site,
        ward_shofco_site,
        COUNT(*) AS total_visits,
        SUM(CASE WHEN specific_reason_for_accessing_space = 'make_swep_products' THEN 1 ELSE 0 END) AS swep_products_visits,
        SUM(CASE WHEN specific_reason_for_accessing_space = 'sewing_machine' THEN 1 ELSE 0 END) AS sewing_machine_visits,
        SUM(CASE WHEN specific_reason_for_accessing_space = 'other' THEN 1 ELSE 0 END) AS other_visits
    FROM source
    GROUP BY year_of_visit, quarter_of_visit, county_shofco_site, subcounty_shofco_site, ward_shofco_site
)

SELECT 
    year_of_visit,
    quarter_of_visit,
    CONCAT('Q', quarter_of_visit, '-', year_of_visit) AS quarter,
    county_shofco_site,
    subcounty_shofco_site,
    ward_shofco_site,
    total_visits,
    swep_products_visits,
    sewing_machine_visits,
    other_visits
FROM aggregated
ORDER BY year_of_visit DESC, quarter_of_visit DESC
