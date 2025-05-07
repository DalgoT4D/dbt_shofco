{{
    config(
        materialized='table',
        alias='sl_mentorship_analytics',
        tags=['sl', 'mentorship', 'analytics']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_mentorship') }}
),

-- Prepare base data with all needed fields
enriched_data AS (
    SELECT
        case_id,
        case_name,
        case_type,
        date_opened,
        mentor_name,
        funding_program,
        mentorship_visit_number,
        solutions_identified,
        agreed_timelines,
        main_challenges,
        -- Extract date components for analysis
        EXTRACT(YEAR FROM date_opened::timestamp) AS year_opened,
        EXTRACT(MONTH FROM date_opened::timestamp) AS month_opened,
        EXTRACT(QUARTER FROM date_opened::timestamp) AS quarter_opened
    FROM source
),

-- Categorize mentorship visits
visit_categorized AS (
    SELECT
        *,
        CASE 
            WHEN mentorship_visit_number IN ('one', '1', 'first') THEN 1
            WHEN mentorship_visit_number IN ('two', '2', 'second') THEN 2
            WHEN mentorship_visit_number IN ('three', '3', 'third') THEN 3
            WHEN mentorship_visit_number IN ('four', '4', 'fourth') THEN 4
            WHEN mentorship_visit_number IN ('five', '5', 'fifth') THEN 5
            ELSE NULL
        END AS visit_number_standardized,
        
        CASE
            WHEN funding_program LIKE '%mcf%' OR funding_program LIKE '%MCF%' THEN 'MCF'
            WHEN funding_program LIKE '%tvet%' OR funding_program LIKE '%TVET%' THEN 'TVET'
            WHEN funding_program LIKE '%youth%' OR funding_program LIKE '%Youth%' THEN 'Youth Program'
            ELSE 'Other/Unknown'
        END AS funding_program_category
    FROM enriched_data
),

-- Add challenge categorization 
challenge_categorized AS (
    SELECT
        *,
        CASE
            WHEN main_challenges ILIKE '%space%' OR main_challenges ILIKE '%inadequate space%' THEN 'Space Issues'
            WHEN main_challenges ILIKE '%insecurity%' OR main_challenges ILIKE '%security%' THEN 'Security Issues'
            WHEN main_challenges ILIKE '%money%' OR main_challenges ILIKE '%fund%' OR main_challenges ILIKE '%capital%' THEN 'Financial Issues'
            WHEN main_challenges ILIKE '%market%' OR main_challenges ILIKE '%customer%' THEN 'Market Access Issues'
            WHEN main_challenges ILIKE '%skill%' OR main_challenges ILIKE '%knowledge%' OR main_challenges ILIKE '%training%' THEN 'Skills/Knowledge Issues'
            ELSE 'Other/Uncategorized Issues'
        END AS challenge_category
    FROM visit_categorized
)

-- Final output table
SELECT * FROM challenge_categorized