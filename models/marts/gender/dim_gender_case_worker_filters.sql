{{ config(
    materialized='table',
    tags=['gender_dim_tables', "gender"]
) }}

WITH unique_combinations AS (
    SELECT DISTINCT
        county,
        site,
        assigned_to,
        reported_by,
        case_is_closed
    FROM {{ ref('case_occurence_pii') }} 
    WHERE 
        county IS NOT NULL
        AND site IS NOT NULL
        AND assigned_to IS NOT NULL
        AND case_is_closed IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY county, site, assigned_to, case_is_closed
    ) AS id,
    county,
    site,
    assigned_to,
    reported_by,
    case_is_closed
FROM unique_combinations
