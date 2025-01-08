{{
    config(
        materialized="table",
        tags='wash_health_indicators'
    )
}}

WITH hygiene_data AS (
    SELECT
        "school_name",
        "term",
        "year",
        "hygiene_score"
    FROM {{ ref('health_indicators') }}
),

schools_with_improvements AS (
    SELECT DISTINCT
        a."school_name",
        a."term",
        a."year"
    FROM hygiene_data a
    INNER JOIN hygiene_data b
    ON a."school_name" = b."school_name"
       AND a."term" = b."term"
       AND a."year" = b."year"
       AND a."form_date" < b."form_date"
       AND a."hygiene_score" < b."hygiene_score"
),

final_aggregates AS (
    SELECT
        h."term",
        h."year",
        COUNT(DISTINCT h."school_name") AS total_no_schools,
        COUNT(DISTINCT s."school_name") AS schools_improved,
        COUNT(DISTINCT s."school_name") * 1.0 / COUNT(DISTINCT h."school_name") AS share_of_schools_improved
    FROM hygiene_data h
    LEFT JOIN schools_with_improvements s
    ON h."school_name" = s."school_name"
       AND h."term" = s."term"
       AND h."year" = s."year"
    GROUP BY h."term", h."year"
)

SELECT * FROM final_aggregates