{{ config(
    materialized='table',
    tags=['sl', 'coworking', 'space_usage', 'earnings', 'visit_log', "sl_marts"]
) }}

SELECT
    case_id,
    user_id,
    
    -- Participant Information (Using all available fields)
    COALESCE(
        NULLIF(TRIM(pp_fullname), ''),
        NULLIF(TRIM(users_fullname), ''),
        NULLIF(TRIM(fullname_csr), ''),
        NULLIF(TRIM(visitor_fullname_csf), ''),
        'Unknown Participant'
    ) AS participant_name,
    pp_unique_id AS unique_id,
    first_name AS first_name,
    second_name AS second_name,
    
    -- Now available fields from rebuilt staging
    NULLIF(TRIM(users_fullname), '') AS users_fullname,
    NULLIF(TRIM(fullname_csr), '') AS fullname_csr,
    NULLIF(TRIM(number_of_visitors), '') AS number_of_visitors,
    NULLIF(TRIM(coworking_space_facility), '') AS coworking_space_facility,
    
    -- Demographics
    CASE WHEN age ~ '^[0-9]+$' THEN age::INT ELSE NULL END AS age,
    sex AS gender,
    nationality AS nationality,
    county AS county,
    
    -- Visit Information
    visitor_fullname_csf AS visitor_name,
    visitors_nationality_csf AS visitor_nationality,
    
    -- PWD Status
    CASE 
        WHEN LOWER(TRIM(is_pwd)) IN ('yes', 'true', '1') THEN 'Yes'
        WHEN LOWER(TRIM(is_pwd)) IN ('no', 'false', '0') THEN 'No'
        ELSE 'Unknown'
    END AS is_pwd,
    
    disability_type_csf AS disability_type,
    is_mother_csf AS is_mother,
    
    -- Training and Services Participated (field not available in staging)
    NULL AS training_or_service_participated,
    
    -- Earning a Living from Coworking
    CASE 
        WHEN LOWER(TRIM(using_coworking_to_earn_a_living_csf)) IN ('yes', 'true', '1') THEN 'Yes'
        WHEN LOWER(TRIM(using_coworking_to_earn_a_living_csf)) IN ('no', 'false', '0') THEN 'No'
        ELSE NULLIF(TRIM(using_coworking_to_earn_a_living_csf), '')
    END AS earning_living_from_coworking,
    
    -- Space Usage Details (All available fields)
    COALESCE(
        NULLIF(TRIM(what_visitor_is_using_csf), ''),
        NULLIF(TRIM(what_visitor_is_using_csr), '')
    ) AS space_usage_purpose,
    
    COALESCE(
        NULLIF(TRIM(purpose_for_using_csf), ''),
        NULLIF(TRIM(specific_reason_for_accessing_space_csr), '')
    ) AS access_reason,
    
    NULLIF(TRIM(specific_reason_for_accessing_csr), '') AS training_or_service_accessed,
    NULLIF(TRIM(other_reason_csr), '') AS other_access_reason,
    
    -- Now available fields from rebuilt staging
    NULLIF(TRIM(purpose_for_using_csf), '') AS purpose_for_using_csf,
    NULLIF(TRIM(training_or_service_csf), '') AS training_or_service_csf,
    NULLIF(TRIM(shofco_training_csf), '') AS shofco_training,
    NULLIF(TRIM(purpose_of_visit), '') AS purpose_of_visit,
    
    -- Contact Information
    primary_phone_number AS phone_number,
    
    -- ID and Registration
    id_number_csf AS id_number,
    asylum_pass_number_csf AS asylum_pass_number,
    registration_document AS registration_document,
    
    -- Visit Timing and Details (Combined from both visit types)
    COALESCE(date_of_visit_csf, date_of_visit_csr) AS date_of_visit,
    date_of_visit_csf AS first_visit_date,
    date_of_visit_csr AS return_visit_date,
    time_in_csf AS time_in,
    time_out_csf AS time_out,
    
    -- Visit Type Classification (Enhanced)
    CASE 
        WHEN date_of_visit_csr IS NOT NULL THEN 'return_visit'
        WHEN date_of_visit_csf IS NOT NULL THEN 'first_visit'
        ELSE 'unknown'
    END AS visit_type,
    
    -- Visit flags for analysis
    CASE WHEN date_of_visit_csf IS NOT NULL THEN TRUE ELSE FALSE END AS is_first_visit,
    CASE WHEN date_of_visit_csr IS NOT NULL THEN TRUE ELSE FALSE END AS is_return_visit,
    
    name_of_facilitator AS facilitator,
    CONCAT(COALESCE(enumerator_first_name, ''), ' ', COALESCE(enumerator_last_name, '')) AS enumerator_full_name,
    enumerator AS enumerator_username,
    
    -- County/Location of Coworking Site
    county_coworking_site_csf AS coworking_site_county,
    subcounty_coworking_site_csf AS coworking_site_subcounty,
    ward_coworking_site_csf AS coworking_site_ward,
    
    -- Refugee Status Analysis
    CASE
        WHEN LOWER(TRIM(citizenship)) IN ('refugee', 'non-kenyan', 'refugee/alien') THEN 'Refugee/Non-Kenyan'
        WHEN LOWER(TRIM(citizenship)) = 'kenyan' THEN 'Kenyan'
        ELSE 'Unknown'
    END AS refugee_status,
    
    -- Age Groups
    CASE
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 18 AND 24 THEN '18-24'
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 25 AND 34 THEN '25-34'
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 35 AND 44 THEN '35-44'
        WHEN age ~ '^[0-9]+$' AND age::INT >= 45 THEN '45+'
        ELSE 'Unknown'
    END AS age_group,
    
    -- Training Categories (field not available, using placeholder)
    'Unknown' AS training_category,
    
    -- Business Usage Flag (Enhanced)
    CASE 
        WHEN LOWER(TRIM(using_coworking_to_earn_a_living_csf)) = 'yes'
             OR LOWER(TRIM(using_coworking_for_a_living_csr)) = 'yes' THEN 'Business User'
        WHEN LOWER(TRIM(using_coworking_to_earn_a_living_csf)) = 'no'
             OR LOWER(TRIM(using_coworking_for_a_living_csr)) = 'no' THEN 'Non-Business User'
        ELSE 'Unknown'
    END AS business_usage_flag,
    
    -- Enhanced earning analysis
    COALESCE(
        NULLIF(TRIM(using_coworking_to_earn_a_living_csf), ''),
        NULLIF(TRIM(using_coworking_for_living_csr), ''),
        NULLIF(TRIM(using_coworking_for_a_living_csr), '')
    ) AS using_for_earning_combined,
    
    -- Metadata
    indexed_on AS date_received,
    date_modified AS last_modified

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    -- Comprehensive filter: Include all coworking records using all identifiers
    date_of_visit_csf IS NOT NULL
    OR date_of_visit_csr IS NOT NULL
    OR time_in_csf IS NOT NULL 
    OR time_out_csf IS NOT NULL
    OR county_coworking_site_csf IS NOT NULL
    OR visitor_fullname_csf IS NOT NULL
    OR what_visitor_is_using_csf IS NOT NULL
    OR what_visitor_is_using_csr IS NOT NULL
    OR users_fullname IS NOT NULL
    OR fullname_csr IS NOT NULL
    OR purpose_for_using_csf IS NOT NULL
    OR training_or_service_csf IS NOT NULL