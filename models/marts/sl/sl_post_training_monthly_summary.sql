{{ config(
    materialized='table',
    schema='dev_sl',
    alias='sl_post_training_monthly_summary',
    tags=['sl', 'post_training', 'summary', 'monthly']
) }}

WITH source AS (
    SELECT * FROM {{ ref('sl_post_training_analytics') }}
),

aggregated AS (
    SELECT
        year_opened,
        month_opened,
        program_donor,
        employment_type,
        COUNT(*) AS total_participants,
        SUM(CASE WHEN is_job_placed THEN 1 ELSE 0 END) AS placed_in_jobs,
        SUM(CASE WHEN is_internship THEN 1 ELSE 0 END) AS placed_in_internships,
        SUM(CASE WHEN is_apprenticeship THEN 1 ELSE 0 END) AS in_apprenticeship,
        SUM(CASE WHEN is_internal_placement THEN 1 ELSE 0 END) AS placed_within_shofco
    FROM source
    GROUP BY year_opened, month_opened, program_donor, employment_type
)

SELECT 
    year_opened,
    month_opened,
    CONCAT(year_opened, '-', LPAD(month_opened::text, 2, '0')) AS month,
    program_donor,
    employment_type,
    total_participants,
    placed_in_jobs,
    placed_in_internships,
    in_apprenticeship,
    placed_within_shofco
FROM aggregated
ORDER BY year_opened DESC, month_opened DESC
