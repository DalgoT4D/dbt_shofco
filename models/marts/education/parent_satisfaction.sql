{{ config(
  materialized='table',
    tags="education_satisfaction_surveys"
) }}

SELECT
    -- Transform term to "Term X"
    INITCAP(REPLACE("term", '_', ' ')) AS "term",

    -- Transform grade to "Kindergarten" or "Grade X"
    CASE
        WHEN LOWER("grade") LIKE '%kindergarten%' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', REGEXP_REPLACE(LOWER("grade"), '.*grade', ''))
    END AS "grade",

    LOWER("school") AS "school_type",
    "education_satisfaction",
    "class_year_of_child" AS "year"
FROM {{ ref("staging_parents_satisfaction_survey") }}