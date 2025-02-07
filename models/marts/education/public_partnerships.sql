{{ config(
  materialized='table',
    tags=["education_expansion", "education"]
) }}

SELECT
    year,
    county,
    mean_kpce_score,
    subcounty,
    teachers_trained,
    students_enrolled,
    computers_provided,
    toilet_stances_built,
    high_touch_low_touch,
    LOWER(school_name)
FROM {{ ref("staging_public_partnerships") }}
