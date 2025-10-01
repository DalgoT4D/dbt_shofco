{{ config(
    materialized='table',
    unique_key='case_id',
    tags=['sl', 'daycare', "sl_marts"]
) }}

SELECT
    case_id,
    
    -- Number of children
    CASE 
        WHEN number_of_children ~ '^[0-9]+$' THEN number_of_children::INT 
        ELSE NULL 
    END AS number_of_children,
    
    -- Children under 4
    CASE 
        WHEN LOWER(TRIM(children_under_4)) IN ('yes', 'true', '1') THEN 'yes'
        WHEN LOWER(TRIM(children_under_4)) IN ('no', 'false', '0') THEN 'no'
        WHEN children_under_4 ~ '^[0-9]+$' AND children_under_4::INT > 0 THEN 'yes'
        WHEN children_under_4 ~ '^[0-9]+$' AND children_under_4::INT = 0 THEN 'no'
        ELSE NULL
    END AS children_under_4,
    
    -- Children with disabilities
    CASE 
        WHEN LOWER(TRIM(children_with_disabilities)) IN ('yes', 'true', '1') THEN 'yes'
        WHEN LOWER(TRIM(children_with_disabilities)) IN ('no', 'false', '0') THEN 'no'
        ELSE NULL
    END AS children_with_disabilities,
    
    -- Daycare support details
    NULLIF(TRIM(children_in_need_of_daycare_support), '') AS daycare_support_details,
    
    -- Children in daycare status (using any children inneed of daycare field)
    NULLIF(TRIM(any_children_inneed_of_daycare_dc), '') AS children_in_daycare_status

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    do_you_have_children = 'yes' 
    OR number_of_children IS NOT NULL 
    OR children_under_4 IS NOT NULL 
    OR children_with_disabilities IS NOT NULL 
    OR children_in_need_of_daycare_support IS NOT NULL