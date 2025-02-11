{{ config(
    materialized='table',
    tags=["education_attendance", "education"]
) }}


WITH clean_data AS (
    SELECT
        "grade",
        "stream",
        "name_of_student",
        "absence_causes",
        "school_type",
        {{ validate_date("date") }} AS "absence_date",
        {{ validate_date("estimated_reporting_date") }} AS "reporting_date"
    FROM {{ ref('staging_followup_attendance') }}
)

SELECT
    {{ validate_date("absence_date") }} AS absence_date,
    EXTRACT(YEAR FROM "absence_date") AS "year",

    -- Calculate term based on the valid date
    CASE 
        WHEN "absence_date" BETWEEN DATE_TRUNC('year', "absence_date")
                          AND DATE_TRUNC('year', "absence_date") + INTERVAL '3 months - 1 day' THEN 'Term 1'
        WHEN "absence_date" BETWEEN DATE_TRUNC('year', "absence_date") + INTERVAL '3 months'
                          AND DATE_TRUNC('year', "absence_date") + INTERVAL '7 months - 1 day' THEN 'Term 2'
        WHEN "absence_date" BETWEEN DATE_TRUNC('year', "absence_date") + INTERVAL '7 months'
                          AND DATE_TRUNC('year', "absence_date") + INTERVAL '12 months - 1 day' THEN 'Term 3'
        ELSE NULL
    END AS "term",

    -- Transform Grade values
    CASE 
        WHEN "grade" = 'K' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', "grade")
    END AS "grade",

    -- Retain other columns and transformations
    "stream",
    "absence_causes",
    {{ validate_date("reporting_date") }} AS "reporting_date",
    LOWER("school_type") AS "school_type",
    COUNT(*) AS "number_of_absences"
FROM clean_data
GROUP BY
    "absence_date",
    CASE 
        WHEN "absence_date" BETWEEN DATE_TRUNC('year', "absence_date")
                          AND DATE_TRUNC('year', "absence_date") + INTERVAL '3 months - 1 day' THEN 'Term 1'
        WHEN "absence_date" BETWEEN DATE_TRUNC('year', "absence_date") + INTERVAL '3 months'
                          AND DATE_TRUNC('year', "absence_date") + INTERVAL '7 months - 1 day' THEN 'Term 2'
        WHEN "absence_date" BETWEEN DATE_TRUNC('year', "absence_date") + INTERVAL '7 months'
                          AND DATE_TRUNC('year', "absence_date") + INTERVAL '12 months - 1 day' THEN 'Term 3'
        ELSE NULL
    END,
    CASE 
        WHEN "grade" = 'K' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', "grade")
    END,
    "stream",
    "absence_causes",
    {{ validate_date("reporting_date" ) }},
    LOWER("school_type")