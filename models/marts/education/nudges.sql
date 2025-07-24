{{ config(
    materialized='table',
    tags=["education_expansion", "education"]
) }}

WITH cleaned_staging_nudges AS (
SELECT
    term,
    CONCAT('Grade ', grade) AS grade,
    cohort,
    year,
    initcap(trim(' ' from county)) as county,
    translate(initcap(trim(' ' from subcounty)),'_,-', ' ') as subcounty,
    gender,
    nudge_type

FROM {{ ref('staging_nudges') }}

)

SELECT
    term,
    grade,
    cohort,
    year,
    COALESCE(NULLIF(county, ''), 'Unknown') AS county,
    COALESCE(NULLIF(subcounty, ''), 'Unknown') AS subcounty,
    gender,
    nudge_type,
    COUNT(*) AS nudge_count
FROM cleaned_staging_nudges
GROUP BY
    term,
    grade,
    year,
    COALESCE(NULLIF(county, ''), 'Unknown'),
    COALESCE(NULLIF(subcounty, ''), 'Unknown'),
    gender,
    nudge_type,
    cohort
