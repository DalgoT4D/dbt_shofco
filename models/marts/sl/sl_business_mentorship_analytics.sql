{{
    config(
        materialized='table',
        alias='sl_business_mentorship_analytics',
        tags=['sl', 'business_mentorship', 'analytics']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_business_mentorship') }}
),

-- Enrich with time dimensions
enriched AS (
    SELECT
        case_id,
        case_name,
        case_type,
        date_opened,
        mentor_name,
        date_of_visit,
        funding_program,
        type_of_business,
        mentorship_visit_number,
        solutions_identified,
        agreed_timelines,
        main_challenges,

        -- Extract year, month, quarter from visit date
        EXTRACT(YEAR FROM date_of_visit::date) AS year_of_visit,
        EXTRACT(MONTH FROM date_of_visit::date) AS month_of_visit,
        EXTRACT(QUARTER FROM date_of_visit::date) AS quarter_of_visit
    FROM source
),

-- Standardize visit numbers and funding program
categorized_visits AS (
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
            WHEN funding_program ILIKE '%mcf%' THEN 'MCF'
            WHEN funding_program ILIKE '%tvet%' THEN 'TVET'
            WHEN funding_program ILIKE '%youth%' THEN 'Youth Program'
            ELSE 'Other/Unknown'
        END AS funding_program_category
    FROM enriched
),

-- Categorize challenges
categorized_challenges AS (
    SELECT
        *,
        CASE
            WHEN main_challenges ILIKE '%space%' THEN 'Space Issues'
            WHEN main_challenges ILIKE '%insecurity%' OR main_challenges ILIKE '%security%' THEN 'Security Issues'
            WHEN main_challenges ILIKE '%fund%' OR main_challenges ILIKE '%capital%' OR main_challenges ILIKE '%money%' THEN 'Financial Issues'
            WHEN main_challenges ILIKE '%market%' OR main_challenges ILIKE '%competition%' THEN 'Market Access Issues'
            WHEN main_challenges ILIKE '%skill%' OR main_challenges ILIKE '%knowledge%' OR main_challenges ILIKE '%training%' THEN 'Skills/Knowledge Issues'
            ELSE 'Other/Uncategorized Issues'
        END AS challenge_category
    FROM categorized_visits
)

SELECT
    case_id,
    case_name,
    case_type,
    date_opened,
    mentor_name,
    CAST(date_of_visit AS DATE) AS date_of_visit,
    funding_program,
    type_of_business,
    mentorship_visit_number,
    solutions_identified,
    agreed_timelines,
    main_challenges,
    year_of_visit,
    month_of_visit,
    quarter_of_visit,
    visit_number_standardized,
    funding_program_category,
    challenge_category
FROM categorized_challenges
