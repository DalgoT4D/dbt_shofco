{{ config(
  materialized='table',
    tags=["education_expansion", "education"]
) }}

WITH cleaned_staging_public_partnerships AS (
SELECT
    year,
    LOWER(school_name) AS school_name,
    mean_kpce_score,
    initcap(trim(' ' from county)) as county,
    translate(initcap(trim(' ' from subcounty)),'_,-', ' ') as subcounty,
    teachers_trained,
    cast(REPLACE(students_enrolled,',','') as INTEGER) as students_enrolled,
    computers_provided,
    toilet_stances_built,
    trim(' ' from high_touch_low_touch) AS high_touch_low_touch

FROM {{ ref("staging_public_partnerships") }}
)

SELECT
    year,
    school_name,
    mean_kpce_score,
    COALESCE(NULLIF(county, ''), 'Unknown') AS county,
    COALESCE(NULLIF(subcounty, ''), 'Unknown') AS subcounty,
    teachers_trained,
    students_enrolled,
    computers_provided,
    toilet_stances_built,
    high_touch_low_touch
FROM cleaned_staging_public_partnerships
