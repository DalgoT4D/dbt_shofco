{{
    config(
        materialized="table" 
    )
}}

WITH hygiene_data AS (
    SELECT
        "school_name",
        "hygiene_score",
        "form_date"
    FROM {{ ref('health_indicators') }}
),

schools_with_improvements AS (
    SELECT DISTINCT
        a."school_name"
    FROM hygiene_data a
    INNER JOIN hygiene_data b
    ON a."school_name" = b."school_name"
        AND a."form_date" < b."form_date"  
        AND a."hygiene_score" < b."hygiene_score"  
),

final_aggregates AS (
    SELECT
        COUNT(DISTINCT h."school_name") AS total_no_schools,
        COUNT(DISTINCT s."school_name") AS schools_improved,
        COUNT(DISTINCT s."school_name") * 1.0 / COUNT(DISTINCT h."school_name") AS share_of_schools_improved
    FROM hygiene_data h
    LEFT JOIN schools_with_improvements s
    ON h."school_name" = s."school_name"
)

SELECT * FROM final_aggregates