{{ config(
    materialized='table',
    tags="wash_water_production"
) }}

WITH lindi_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "BH_Abstraction" as "bh_production",
    "TREATED_Production" as "treated_consumption",
    'Lindi' as "tank"
  FROM {{ source('staging_wash', 'Lindi') }}
  WHERE CAST("BH_Abstraction" AS NUMERIC) >= 0 
  AND "BH_METER" IS NOT NULL AND CAST("TREATED_Production" AS NUMERIC) >= 0
),

laini_saba_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "BH_Abstraction" as "bh_production",
    "TREATED_Production" as "treated_consumption",
    'Laini Saba' as "tank"
  FROM {{ source('staging_wash', 'Laini_Saba') }}
  WHERE CAST("BH_Abstraction" AS NUMERIC) >= 0 
  AND "BH_METER" IS NOT NULL AND CAST("TREATED_Production" AS NUMERIC) >= 0
),

hq_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "BH_Abstraction" as "bh_production",
    "TREATED_PRODUCTION" as "treated_consumption",
    'HQ' as "tank"
  FROM {{ source('staging_wash', 'HQ') }}
  WHERE CAST("BH_Abstraction" AS NUMERIC) >= 0 
  AND "BH_METER" IS NOT NULL AND CAST("TREATED_PRODUCTION" AS NUMERIC) >= 0
),

makina_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    "BH_Abstraction" as "bh_production",
    "TREATEDProduction" as "treated_consumption",
    'Makina' as "tank"
  FROM {{ source('staging_wash', 'Makina') }}
  WHERE CAST("BH_Abstraction" AS NUMERIC) >= 0 
  AND "BH_METER" IS NOT NULL AND CAST("TREATEDProduction" AS NUMERIC) >= 0
),

subra_cte as
(
  SELECT
    "DATE" as "date",
    "Status" as "status",
    CAST((CAST("B2_Abstraction" AS NUMERIC) + CAST("BH1_Abstraction" AS NUMERIC)) AS VARCHAR) as "bh_production",
    "TREATED_Production" as "treated_consumption",
    'Subra' as "tank"
  FROM {{ source('staging_wash', 'Subra') }}
  WHERE CAST("B2_Abstraction" AS NUMERIC) + CAST("BH1_Abstraction" AS NUMERIC) >= 0
  AND "BH_1_METER" IS NOT NULL AND CAST("TREATED_Production" AS NUMERIC) >= 0
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