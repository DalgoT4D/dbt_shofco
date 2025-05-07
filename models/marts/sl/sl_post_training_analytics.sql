{{ 
    config(
        materialized='table',
        alias='sl_post_training_analytics',
        tags=['sl', 'post_training', 'analytics']
    ) 
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_post_training') }}
),

processed AS (
    SELECT
        case_id,
        case_name,
        case_type,
        date_opened,
        officer_name,
        job_placement,
        program_donor,
        employment_type,
        training_activity,
        internship_placement,
        position,
        placement_within_shofco,
        apprenticeship_opportunities,
        placement_organization,

        -- Categorized donor for grouping
        CASE
            WHEN program_donor ILIKE '%mcf%' THEN 'MCF'
            WHEN program_donor ILIKE '%youth%' THEN 'Youth Program'
            WHEN program_donor ILIKE '%tvet%' THEN 'TVET'
            ELSE 'Other/Unknown'
        END AS donor_category,

        -- Convert 'yes'/'no' to booleans
        job_placement = 'yes' AS is_job_placed,
        internship_placement = 'yes' AS is_internship,
        apprenticeship_opportunities = 'yes' AS is_apprenticeship,
        placement_within_shofco = 'yes' AS is_internal_placement,

        -- Extract date components
        EXTRACT(YEAR FROM date_opened::timestamp) AS year_opened,
        EXTRACT(MONTH FROM date_opened::timestamp) AS month_opened,
        EXTRACT(QUARTER FROM date_opened::timestamp) AS quarter_opened
    FROM source
)

SELECT * FROM processed
