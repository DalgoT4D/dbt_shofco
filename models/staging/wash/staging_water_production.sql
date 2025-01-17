{{ config(
    materialized='table',
    tags="wash_water_production"
) }}

WITH lindi_cte AS
(
    SELECT
        "DATE" AS date,
        "Status" AS status,
        "BH_Abstraction" AS bh_production,
        "TREATED_Production" AS treated_consumption,
        'Lindi' AS tank
    FROM {{ source('staging_wash', 'Lindi') }}
    WHERE
        CAST("BH_Abstraction" AS NUMERIC) >= 0 
        AND "BH_METER" IS NOT NULL AND CAST("TREATED_Production" AS NUMERIC) >= 0
),

laini_saba_cte AS
(
    SELECT
        "DATE" AS date,
        "Status" AS status,
        "BH_Abstraction" AS bh_production,
        "TREATED_Production" AS treated_consumption,
        'Laini Saba' AS tank
    FROM {{ source('staging_wash', 'Laini_Saba') }}
    WHERE
        CAST("BH_Abstraction" AS NUMERIC) >= 0 
        AND "BH_METER" IS NOT NULL AND CAST("TREATED_Production" AS NUMERIC) >= 0
),

hq_cte AS
(
    SELECT
        "DATE" AS date,
        "Status" AS status,
        "BH_Abstraction" AS bh_production,
        "TREATED_PRODUCTION" AS treated_consumption,
        'HQ' AS tank
    FROM {{ source('staging_wash', 'HQ') }}
    WHERE
        CAST("BH_Abstraction" AS NUMERIC) >= 0 
        AND "BH_METER" IS NOT NULL AND CAST("TREATED_PRODUCTION" AS NUMERIC) >= 0
),

makina_cte AS
(
    SELECT
        "DATE" AS date,
        "Status" AS status,
        "BH_Abstraction" AS bh_production,
        "TREATEDProduction" AS treated_consumption,
        'Makina' AS tank
    FROM {{ source('staging_wash', 'Makina') }}
    WHERE
        CAST("BH_Abstraction" AS NUMERIC) >= 0 
        AND "BH_METER" IS NOT NULL AND CAST("TREATEDProduction" AS NUMERIC) >= 0
),

subra_cte AS
(
    SELECT
        "DATE" AS date,
        "Status" AS status,
        CAST((CAST("B2_Abstraction" AS NUMERIC) + CAST("BH1_Abstraction" AS NUMERIC)) AS VARCHAR) AS bh_production,
        "TREATED_Production" AS treated_consumption,
        'Subra' AS tank
    FROM {{ source('staging_wash', 'Subra') }}
    WHERE
        CAST("B2_Abstraction" AS NUMERIC) + CAST("BH1_Abstraction" AS NUMERIC) >= 0
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
