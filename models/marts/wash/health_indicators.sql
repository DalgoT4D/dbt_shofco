{{ config(
    materialized='table',
    tags='wash_health_indicators'
) }}

SELECT
    "id",
    "form_date",
    "term",
    "school_name",
    "health_club_boys",
    "health_club_girls",
    "health_club_active",
    "hygiene_score"
FROM {{ ref('staging_health_indicators') }}
WHERE "health_club_active"=1