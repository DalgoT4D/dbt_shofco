{{ config(
    materialized='table',
    tags='wash_health_indicators'
) }}

SELECT
    *
FROM {{ ref('staging_health_indicators') }}
WHERE "health_club_active"=1