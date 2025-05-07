{{ config(
    materialized='table',
    schema='dev_sl',
    alias='sl_post_training_quarterly_summary',
    tags=['sl', 'post_training', 'summary', 'quarterly']
) }}

WITH source AS (
    SELECT * FROM {{ ref('sl_post_training_analytics') }}
),

aggregated AS (
    SELECT
        year_opened,
        quarter_opened,
        program_donor,
        employment_type,
        COUNT(*) AS total_participants,
        SUM(CASE WHEN is_job_placed THEN 1 ELSE 0 END) AS placed_in_jobs,
        SUM(CASE WHEN is_internship THEN 1 ELSE 0 END) AS placed_in_internships,
        SUM(CASE WHEN is_apprenticeship THEN 1 ELSE 0 END) AS in_apprenticeship,
        SUM(CASE WHEN is_internal_placement THEN 1 ELSE 0 END) AS placed_within_shofco
    FROM source
    GROUP BY year_opened, quarter_opened, program_donor, employment_type
)

SELECT 
    year_opened,
    quarter_opened,
    CONCAT('Q', quarter_opened, '-', year_opened) AS quarter,
    program_donor,
    employment_type,
    total_participants,
    placed_in_jobs,
    placed_in_internships,
    in_apprenticeship,
    placed_within_shofco
FROM aggregated
ORDER BY year_opened DESC, quarter_opened DESC
