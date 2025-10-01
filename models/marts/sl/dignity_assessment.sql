{{ config(
    materialized='table',
    unique_key='case_id',
    tags=['sl', 'dignity_kit', "sl_marts"]
) }}

SELECT
    case_id,
    
    -- Experienced days where you had nothing
    CASE 
        WHEN LOWER(TRIM(experienced_days_where_you_had_nothing_sr)) IN ('yes', 'true', '1') 
             OR LOWER(TRIM(experienced_days_where_you_had_nothing_uf)) IN ('yes', 'true', '1') THEN 'yes'
        WHEN LOWER(TRIM(experienced_days_where_you_had_nothing_sr)) IN ('no', 'false', '0') 
             OR LOWER(TRIM(experienced_days_where_you_had_nothing_uf)) IN ('no', 'false', '0') THEN 'no'
        ELSE NULL
    END AS experienced_days_where_you_had_nothing,
    
    -- Miss school due to lack of hygiene products
    CASE 
        WHEN LOWER(TRIM(miss_school_due_to_lack_hygiene_products_sr)) IN ('yes', 'true', '1') 
             OR LOWER(TRIM(miss_school_due_to_lack_hygiene_products_uf)) IN ('yes', 'true', '1') THEN 'yes'
        WHEN LOWER(TRIM(miss_school_due_to_lack_hygiene_products_sr)) IN ('no', 'false', '0') 
             OR LOWER(TRIM(miss_school_due_to_lack_hygiene_products_uf)) IN ('no', 'false', '0') THEN 'no'
        ELSE NULL
    END AS miss_school_due_to_lack_hygiene_products,
    
    -- Challenges accessing personal hygiene items (already coalesced in staging)
    CASE 
        WHEN LOWER(TRIM(challenges_accessing_personal_hygiene_items)) IN ('yes', 'true', '1') THEN 'yes'
        WHEN LOWER(TRIM(challenges_accessing_personal_hygiene_items)) IN ('no', 'false', '0') THEN 'no'
        ELSE NULL
    END AS challenges_accessing_personal_hygiene_items,
    
    -- Dignity kit support
    NULLIF(TRIM(dignity_kit_support), '') AS dignity_kit_support,
    
    -- Head gender
    COALESCE(
        NULLIF(TRIM(head_gender_sr), ''),
        NULLIF(TRIM(head_gender_uf), '')
    ) AS head_gender,
    
    -- Receiving aid (already coalesced in staging)
    CASE 
        WHEN LOWER(TRIM(receiving_aid)) IN ('yes', 'true', '1') THEN 'yes'
        WHEN LOWER(TRIM(receiving_aid)) IN ('no', 'false', '0') THEN 'no'
        ELSE NULL
    END AS receiving_aid,
    
    -- Household head (already coalesced in staging)
    NULLIF(TRIM(household_head), '') AS household_head,
    
    -- HH engaged in work
    COALESCE(
        NULLIF(TRIM(hh_engaged_in_work_sr), ''),
        NULLIF(TRIM(hh_engaged_in_work_uf), '')
    ) AS hh_engaged_in_work,
    
    -- Forego basic essentials
    COALESCE(
        NULLIF(TRIM(forego_basic_essentials_sr), ''),
        NULLIF(TRIM(forego_basic_essentials_uf), '')
    ) AS forego_basic_essentials,
    
    -- Number of meals reduced
    COALESCE(
        NULLIF(TRIM(number_of_meals_reduced_sr), ''),
        NULLIF(TRIM(number_of_meals_reduced_uf), '')
    ) AS number_of_meals_reduced

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    dignity_kit_support IS NOT NULL
    OR experienced_days_where_you_had_nothing_sr IS NOT NULL
    OR experienced_days_where_you_had_nothing_uf IS NOT NULL
    OR miss_school_due_to_lack_hygiene_products_sr IS NOT NULL
    OR miss_school_due_to_lack_hygiene_products_uf IS NOT NULL
    OR challenges_accessing_personal_hygiene_items IS NOT NULL