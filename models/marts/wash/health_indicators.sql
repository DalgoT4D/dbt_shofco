{{ config(
    materialized='table',
    tags=['wash_health_indicators',"wash"]
) }}

SELECT
    id,
    {{ validate_date("form_date") }} AS date,
    term,
    school_name,
    health_club_boys,
    health_club_girls,
    health_club_active,
    hygiene_score,
    CASE
        WHEN form_date IS NOT NULL THEN EXTRACT(YEAR FROM {{ validate_date("form_date") }})
    END AS year
FROM {{ ref('staging_health_indicators') }}
WHERE health_club_active=1
