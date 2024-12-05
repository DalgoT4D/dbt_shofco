{{ config(
    materialized='table'
) }}

SELECT
    CAST("term" AS TEXT) AS "term",
    CAST("schools_with_health_clubs" AS INTEGER) AS "schools_with_health_clubs",
    CAST("student_health_club_participants" AS INTEGER) AS "student_health_club_participants",
    CAST("avg_hygiene_score" AS NUMERIC(5, 2)) AS "avg_hygiene_score",
    CAST("hygiene_score_improvement" AS NUMERIC(5, 2)) AS "hygiene_score_improvement"
FROM {{ ref('staging_health_indicators') }}