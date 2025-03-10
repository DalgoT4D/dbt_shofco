{{ config(
    materialized='table',
    tags=["education_expansion", "education"]
) }}

SELECT
    term,
    CONCAT('Grade ', grade) AS grade,
    cohort,
    year,
    county,
    subcounty,
    gender,
    nudge_type,
    COUNT(*) AS nudge_count
FROM {{ ref('staging_nudges') }}
GROUP BY
    term,
    CONCAT('Grade ', grade),
    year,
    county,
    subcounty,
    gender,
    nudge_type,
    cohort
