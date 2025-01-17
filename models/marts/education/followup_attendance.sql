{{ config(
    materialized='table',
    tags="education_attendance"
) }}

WITH clean_data AS (
    SELECT
        grade,
        stream,
        name_of_student,
        absence_causes,
        _airbyte_extracted_at,
        school_type,
        -- Directly convert "Date_" to a proper date
        TO_DATE(date, 'DD/MM/YYYY') AS absence_date,
        TO_DATE(estimated_reporting_date, 'DD/MM/YYYY') AS reporting_date
    FROM {{ ref('staging_followup_attendance') }}
)

SELECT
    absence_date,
    EXTRACT(YEAR FROM absence_date) AS year,

    -- Calculate term based on the valid date
    CASE 
        WHEN
            absence_date BETWEEN DATE_TRUNC('year', absence_date)
            AND DATE_TRUNC('year', absence_date) + INTERVAL '3 months - 1 day' THEN 'Term 1'
        WHEN
            absence_date BETWEEN DATE_TRUNC('year', absence_date) + INTERVAL '3 months'
            AND DATE_TRUNC('year', absence_date) + INTERVAL '7 months - 1 day' THEN 'Term 2'
        WHEN
            absence_date BETWEEN DATE_TRUNC('year', absence_date) + INTERVAL '7 months'
            AND DATE_TRUNC('year', absence_date) + INTERVAL '12 months - 1 day' THEN 'Term 3'
    END AS term,

    -- Transform Grade values
    CASE 
        WHEN grade = 'K' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', grade)
    END AS grade,

    -- Retain other columns and transformations
    stream,
    absence_causes,
    reporting_date,
    LOWER(school_type) AS school_type,
    COUNT(*) AS number_of_absences
FROM clean_data
GROUP BY
    absence_date,
    CASE 
        WHEN
            absence_date BETWEEN DATE_TRUNC('year', absence_date)
            AND DATE_TRUNC('year', absence_date) + INTERVAL '3 months - 1 day' THEN 'Term 1'
        WHEN
            absence_date BETWEEN DATE_TRUNC('year', absence_date) + INTERVAL '3 months'
            AND DATE_TRUNC('year', absence_date) + INTERVAL '7 months - 1 day' THEN 'Term 2'
        WHEN
            absence_date BETWEEN DATE_TRUNC('year', absence_date) + INTERVAL '7 months'
            AND DATE_TRUNC('year', absence_date) + INTERVAL '12 months - 1 day' THEN 'Term 3'
    END,
    CASE 
        WHEN grade = 'K' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', grade)
    END,
    stream,
    absence_causes,
    reporting_date,
    LOWER(school_type)
