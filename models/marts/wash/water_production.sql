{{ config(
  materialized='table',
  tags=['wash_water_production', "wash"]
) }}

select
    {{ validate_date("date_extracted") }} as "date",
    "status",
    "bh_production",
    "treated_consumption",
    "tank",
    CASE
        WHEN {{ validate_date("date_extracted") }} IS NOT NULL THEN EXTRACT(YEAR FROM {{ validate_date("date_extracted") }})
        ELSE NULL
    END AS "year"
FROM {{ ref('staging_water_production') }}