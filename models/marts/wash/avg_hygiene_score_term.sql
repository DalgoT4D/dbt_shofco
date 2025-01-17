{{ config(
    materialized='table',
    tags='wash_health_indicators'
) }}

WITH latest_scores AS (
    SELECT
        school_name,
        term,
        year,
        MAX(CAST(date AS DATE)) AS latest_date
    FROM {{ ref('health_indicators') }}
    GROUP BY school_name, term, year
),

filtered_scores AS (
    SELECT
        h.school_name,
        h.term,
        h.year,
        h.hygiene_score
    FROM {{ ref('health_indicators') }} AS h
    INNER JOIN latest_scores AS l
        ON
            h.school_name = l.school_name
            AND h.term = l.term
            AND h.year = l.year
            AND CAST(h.date AS DATE) = l.latest_date
),

average_scores AS (
    SELECT
        term,
        year,
        AVG(hygiene_score) AS avg_hygiene_score
    FROM filtered_scores
    GROUP BY term, year
)

SELECT * FROM average_scores
