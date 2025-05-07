{{
    config(
        materialized='table',
        alias='sl_tvet_enrolment_analytics',
        tags=['sl', 'tvet', 'analytics']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_tvet_enrolment') }}
),

enriched_data AS (
    SELECT
        case_id,
        case_name,
        case_type,
        date_opened,
        parent_name,
        parent_phone_number,
        officer_name,
        registered_by,
        tvet_cohort,
        tvet_start_date,
        tvet_completion_date,
        tvet_course_enrolled,
        tvet_participant_program,
        tvet_school_placed,
        county_of_school_placed,
        other_institution,
        specified_course,
        date_of_registration,

        -- Date breakdowns
        EXTRACT(YEAR FROM tvet_start_date::date) AS start_year,
        EXTRACT(MONTH FROM tvet_start_date::date) AS start_month,
        EXTRACT(YEAR FROM tvet_completion_date::date) AS end_year,
        EXTRACT(MONTH FROM tvet_completion_date::date) AS end_month

    FROM source
),

categorized AS (
    SELECT *,
        CASE
            WHEN tvet_participant_program ILIKE '%mcf%' THEN 'MCF'
            WHEN tvet_participant_program ILIKE '%youth%' THEN 'Youth Program'
            WHEN tvet_participant_program ILIKE '%tvet%' THEN 'TVET'
            ELSE 'Other/Unknown'
        END AS participant_program_category
    FROM enriched_data
)

SELECT * FROM categorized
