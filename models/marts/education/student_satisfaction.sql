{{ config(
  materialized='table',
    tags=["education_satisfaction_surveys", "education"]
) }}

SELECT
    -- Transform term to "Term X"
    school_satisfaction,

    -- Transform grade to "Kindergarten" or "Grade X"
    class_year AS year,

    INITCAP(REPLACE(term, 'term', 'Term ')) AS term,
    CASE
        WHEN LOWER(grade) LIKE '%kindergarten%' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', REGEXP_REPLACE(LOWER(grade), '.*grade', ''))
    END AS grade,
    LOWER(school) AS school_type
FROM {{ ref("staging_student_satisfaction_survey") }}
