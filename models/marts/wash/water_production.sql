{{ config(
  materialized='table',
  tags=['wash_water_production', "wash"]
) }}

select
    "date",
    "status",
    "bh_production",
    "treated_consumption",
    "tank",
    CASE
        WHEN "date" IS NOT NULL THEN EXTRACT(YEAR FROM {{ validate_date("date") }})
        ELSE NULL
    END AS "year"
FROM {{ ref('staging_water_production') }}