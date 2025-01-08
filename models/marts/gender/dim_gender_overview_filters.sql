{{ config(
    materialized='table' 
) }}

WITH unique_combinations AS (
    SELECT DISTINCT
        case_county_name,
        gender_site_name_of_reporting,
        assigned_to,
        case_is_closed
    FROM {{ ref('case_occurence') }} 
    WHERE 
        case_county_name IS NOT NULL AND
        gender_site_name_of_reporting IS NOT NULL AND
        assigned_to IS NOT NULL AND
        case_is_closed IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY case_county_name, gender_site_name_of_reporting, assigned_to, case_is_closed) AS id,
    case_county_name,
    gender_site_name_of_reporting,
    assigned_to,
    case_is_closed
FROM unique_combinations