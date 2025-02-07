{{ config(
  materialized='table',
    tags=["education_satisfaction_surveys", "education"]
) }}

SELECT
    -- Transform term to "Term X"
    education_satisfaction,

    -- Transform grade to "Kindergarten" or "Grade X"
    class_year_of_child AS year,

    INITCAP(REPLACE(term, '_', ' ')) AS term,
    CASE
        WHEN LOWER(grade) LIKE '%kindergarten%' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', REGEXP_REPLACE(LOWER(grade), '.*grade', ''))
    END AS grade,
    LOWER(school) AS school_type
FROM {{ ref("staging_parents_satisfaction_survey") }}
