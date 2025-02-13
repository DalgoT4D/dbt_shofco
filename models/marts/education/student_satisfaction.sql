{{ config(
  materialized='table',
    tags=["education_satisfaction_surveys", "education"]
) }}

SELECT
    -- Transform term to "Term X"
    INITCAP(REPLACE("term", 'term', 'Term ')) AS "term",

    -- Transform grade to "Kindergarten" or "Grade X"
    CASE
        WHEN LOWER("grade") LIKE '%kindergarten%' THEN 'Kindergarten'
        ELSE CONCAT('Grade ', REGEXP_REPLACE(LOWER("grade"), '.*grade', ''))
    END AS "grade",

    LOWER("school") AS "school_type",
    "school_satisfaction",
    "class_year" AS "year",
    {{ validate_date("submission_time") }} AS "submission_time"
FROM {{ ref("staging_student_satisfaction_survey") }}
