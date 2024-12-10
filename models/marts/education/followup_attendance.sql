{{ config(
    materialized='table'
) }}

WITH clean_data AS (
    SELECT
        *,
        -- Directly convert "Date_" to a proper date
        TO_DATE("Date_", 'DD/MM/YYYY') AS valid_date,
        TO_DATE("Estimated_reporting_date", 'DD/MM/YYYY') AS valid_reporting_date
    FROM {{ ref('staging_followup_attendance') }}
)

SELECT
    -- Use valid_date for absence_date
    valid_date AS "absence_date",
    -- Extract year from valid_date
    EXTRACT(YEAR FROM valid_date) AS "year",

    -- Calculate term based on the valid date
    CASE 
        WHEN valid_date BETWEEN DATE_TRUNC('year', valid_date)
                          AND DATE_TRUNC('year', valid_date) + INTERVAL '3 months - 1 day' THEN 'Term 1'
        WHEN valid_date BETWEEN DATE_TRUNC('year', valid_date) + INTERVAL '3 months'
                          AND DATE_TRUNC('year', valid_date) + INTERVAL '7 months - 1 day' THEN 'Term 2'
        WHEN valid_date BETWEEN DATE_TRUNC('year', valid_date) + INTERVAL '7 months'
                          AND DATE_TRUNC('year', valid_date) + INTERVAL '12 months - 1 day' THEN 'Term 3'
        ELSE NULL
    END AS "term",

    -- Transform Grade values
    CASE 
        WHEN "Grade" = 'K' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', "Grade")
    END AS "grade",

    -- Retain other columns and transformations
    "Stream" AS "stream",
    "Reason_for_absenteesm" AS "absence_causes",
    valid_reporting_date AS "reporting_date",
    LOWER("school_type") AS "school_type",

    -- Count absences
    COUNT(*) AS "number_of_absences"
FROM clean_data
GROUP BY
    valid_date,
    CASE 
        WHEN valid_date BETWEEN DATE_TRUNC('year', valid_date)
                          AND DATE_TRUNC('year', valid_date) + INTERVAL '3 months - 1 day' THEN 'Term 1'
        WHEN valid_date BETWEEN DATE_TRUNC('year', valid_date) + INTERVAL '3 months'
                          AND DATE_TRUNC('year', valid_date) + INTERVAL '7 months - 1 day' THEN 'Term 2'
        WHEN valid_date BETWEEN DATE_TRUNC('year', valid_date) + INTERVAL '7 months'
                          AND DATE_TRUNC('year', valid_date) + INTERVAL '12 months - 1 day' THEN 'Term 3'
        ELSE NULL
    END,
    CASE 
        WHEN "Grade" = 'K' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', "Grade")
    END,
    "Stream",
    "Reason_for_absenteesm",
    valid_reporting_date,
    LOWER("school_type")