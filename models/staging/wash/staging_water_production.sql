{{ config(
    materialized='table',
    tags="wash_water_production"
) }}

WITH lindi_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "BH_Production" as "bh_production",
    "TREATED_Consumption" as "treated_consumption",
    'Lindi' as "tank"
  FROM {{ source('staging_wash', 'Lindi') }}
  WHERE CAST("BH_Production" AS NUMERIC) >= 0 
  AND "BH_METER" IS NOT NULL AND CAST("TREATED_Consumption" AS NUMERIC) >= 0
),

laini_saba_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "BH_Production" as "bh_production",
    "TREATED_Consumption" as "treated_consumption",
    'Laini Saba' as "tank"
  FROM {{ source('staging_wash', 'Laini_Saba') }}
  WHERE CAST("BH_Production" AS NUMERIC) >= 0 
  AND "BH_METER" IS NOT NULL AND CAST("TREATED_Consumption" AS NUMERIC) >= 0
),

hq_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "BH_Production" as "bh_production",
    "TREATED_CONSUMPTION" as "treated_consumption",
    'HQ' as "tank"
  FROM {{ source('staging_wash', 'HQ') }}
  WHERE CAST("BH_Production" AS NUMERIC) >= 0 
  AND "BH_METER" IS NOT NULL AND CAST("TREATED_CONSUMPTION" AS NUMERIC) >= 0
),

makina_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "BH_Production" as "bh_production",
    "TREATED_CONSUMPTION" as "treated_consumption",
    'Makina' as "tank"
  FROM {{ source('staging_wash', 'Makina') }}
  WHERE CAST("BH_Production" AS NUMERIC) >= 0 
  AND "BH_METER" IS NOT NULL AND CAST("TREATED_CONSUMPTION" AS NUMERIC) >= 0
),

subra_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "TOTAL_BH_Production" as "bh_production",
    "TREATED_CONSUMPTION" as "treated_consumption",
    'Subra' as "tank"
  FROM {{ source('staging_wash', 'Subra') }}
  WHERE CAST("TOTAL_BH_Production" AS NUMERIC) >= 0 
  AND "BH_1_METER" IS NOT NULL AND CAST("TREATED_CONSUMPTION" AS NUMERIC) >= 0
)

SELECT *
FROM lindi_cte

UNION ALL

SELECT *
FROM laini_saba_cte

UNION ALL

SELECT *
FROM hq_cte

UNION ALL

SELECT *
FROM makina_cte

UNION ALL

SELECT *
FROM subra_cte