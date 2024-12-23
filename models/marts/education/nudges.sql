{{ config(
    materialized='table',
    tags="education_expansion"
) }}

SELECT
    "Term" AS "term",
    CONCAT('Grade ', "CURRENT_GRADE") AS "grade",
    "Year" AS "year",
    LOWER("COUNTY") AS "county",
    LOWER("SUBCOUNTY") AS "subcounty",
    LOWER("GENDER") AS "gender",
    "NUDGE_TYPE_RECEIVED" AS "nudge_type",
    COUNT(*) AS "nudge_count"
FROM {{ ref('staging_nudges') }}
GROUP BY
    "Term",
    CONCAT('Grade ', "CURRENT_GRADE"),
    "Year",
    LOWER("COUNTY"),
    LOWER("SUBCOUNTY"),
    LOWER("GENDER"),
    "NUDGE_TYPE_RECEIVED"