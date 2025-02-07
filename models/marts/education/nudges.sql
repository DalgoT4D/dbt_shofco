{{ config(
    materialized='table',
    tags=["education_expansion", "education"]
) }}

SELECT
    term,
    cohort
    AS year,
    county,
    subcounty,
    gender,
    nudge_type,
    CONCAT('Grade ', grade) AS grade,
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
