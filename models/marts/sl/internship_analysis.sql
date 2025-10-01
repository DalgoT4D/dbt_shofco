{{ config(
    materialized='table',
    tags=['sl', 'internship', 'placement', 'sectors', "sl_marts"]
) }}

SELECT
    case_id,
    user_id,
    pp_fullname AS participant_name,
    pp_unique_id AS unique_id,
    
    -- Demographics
    CASE WHEN age ~ '^[0-9]+$' THEN age::INT ELSE NULL END AS age,
    sex AS gender,
    county AS county,
    
    -- Internship Details
    NULLIF(TRIM(internship_position_int), '') AS internship_position,
    NULLIF(TRIM(name_of_organization_int), '') AS organization_name,
    NULLIF(TRIM(sector_of_the_organization_int), '') AS organization_sector,
    NULLIF(TRIM(other_sector_int), '') AS other_sector_details,
    
    -- Supervisor Information
    NULLIF(TRIM(supervisors_name_int), '') AS supervisor_name,
    NULLIF(TRIM(supervisor_phone_number_int), '') AS supervisor_phone,
    
    -- Dates and Duration
    {{ validate_date('start_date_int') }} AS internship_start_date,
    {{ validate_date('completion_date_int') }} AS internship_completion_date,
    
    -- Calculate internship duration in days
    CASE 
        WHEN {{ validate_date('start_date_int') }} IS NOT NULL 
             AND {{ validate_date('completion_date_int') }} IS NOT NULL
        THEN {{ validate_date('completion_date_int') }} - {{ validate_date('start_date_int') }}
        ELSE NULL
    END AS internship_duration_days,
    
    -- Internal vs External Placement
    CASE 
        WHEN LOWER(TRIM(placement_within_shofco_int)) IN ('yes', 'true', '1') THEN 'Internal (SHOFCO)'
        WHEN LOWER(TRIM(placement_within_shofco_int)) IN ('no', 'false', '0') THEN 'External'
        ELSE NULLIF(TRIM(placement_within_shofco_int), '')
    END AS placement_type,
    
    -- SHOFCO Program Information
    NULLIF(TRIM(shofco_program_int), '') AS shofco_program,
    NULLIF(TRIM(specify_other_int), '') AS other_program_details,
    
    -- Location Information (using coalesced county field)
    county AS internship_county,
    NULLIF(TRIM(regions_int), '') AS internship_region,
    
    -- Staff Information
    NULLIF(TRIM(staff_name_int), '') AS staff_name,
    
    -- Training Activity
    NULLIF(TRIM(training_activity_int), '') AS training_activity,
    
    -- Sector Classification (standardized)
    CASE 
        WHEN LOWER(TRIM(sector_of_the_organization_int)) LIKE '%education%' 
             OR LOWER(TRIM(sector_of_the_organization_int)) LIKE '%school%' THEN 'Education'
        WHEN LOWER(TRIM(sector_of_the_organization_int)) LIKE '%health%' 
             OR LOWER(TRIM(sector_of_the_organization_int)) LIKE '%medical%' THEN 'Healthcare'
        WHEN LOWER(TRIM(sector_of_the_organization_int)) LIKE '%ngo%' 
             OR LOWER(TRIM(sector_of_the_organization_int)) LIKE '%non-profit%' THEN 'NGO/Non-Profit'
        WHEN LOWER(TRIM(sector_of_the_organization_int)) LIKE '%government%' 
             OR LOWER(TRIM(sector_of_the_organization_int)) LIKE '%public%' THEN 'Government/Public'
        WHEN LOWER(TRIM(sector_of_the_organization_int)) LIKE '%business%' 
             OR LOWER(TRIM(sector_of_the_organization_int)) LIKE '%private%' THEN 'Private Sector/Business'
        WHEN LOWER(TRIM(sector_of_the_organization_int)) LIKE '%technology%' 
             OR LOWER(TRIM(sector_of_the_organization_int)) LIKE '%it%' THEN 'Technology/IT'
        WHEN LOWER(TRIM(sector_of_the_organization_int)) LIKE '%finance%' 
             OR LOWER(TRIM(sector_of_the_organization_int)) LIKE '%bank%' THEN 'Finance/Banking'
        ELSE COALESCE(NULLIF(TRIM(sector_of_the_organization_int), ''), 'Other/Unspecified')
    END AS sector_category,
    
    -- Duration Categories
    CASE 
        WHEN {{ validate_date('start_date_int') }} IS NOT NULL 
             AND {{ validate_date('completion_date_int') }} IS NOT NULL THEN
            CASE 
                WHEN ({{ validate_date('completion_date_int') }} - {{ validate_date('start_date_int') }}) <= 30 THEN '1 Month or Less'
                WHEN ({{ validate_date('completion_date_int') }} - {{ validate_date('start_date_int') }}) <= 90 THEN '1-3 Months'
                WHEN ({{ validate_date('completion_date_int') }} - {{ validate_date('start_date_int') }}) <= 180 THEN '3-6 Months'
                WHEN ({{ validate_date('completion_date_int') }} - {{ validate_date('start_date_int') }}) <= 365 THEN '6 Months - 1 Year'
                ELSE 'More than 1 Year'
            END
        ELSE 'Duration Unknown'
    END AS duration_category,
    
    -- Age Groups
    CASE
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 18 AND 24 THEN '18-24'
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 25 AND 34 THEN '25-34'
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 35 AND 44 THEN '35-44'
        WHEN age ~ '^[0-9]+$' AND age::INT >= 45 THEN '45+'
        ELSE 'Unknown'
    END AS age_group,
    
    -- Refugee Status
    CASE
        WHEN LOWER(TRIM(citizenship)) IN ('refugee', 'non-kenyan', 'refugee/alien') THEN 'Refugee/Non-Kenyan'
        WHEN LOWER(TRIM(citizenship)) = 'kenyan' THEN 'Kenyan'
        ELSE 'Unknown'
    END AS refugee_status,
    
    -- Completion Status
    CASE 
        WHEN {{ validate_date('completion_date_int') }} IS NOT NULL THEN 'Completed'
        WHEN {{ validate_date('start_date_int') }} IS NOT NULL 
             AND {{ validate_date('completion_date_int') }} IS NULL THEN 'In Progress/Incomplete'
        ELSE 'Status Unknown'
    END AS completion_status,
    
    -- Metadata
    indexed_on AS date_received,
    date_modified AS last_modified

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    -- Include internship records
    internship_position_int IS NOT NULL 
    OR name_of_organization_int IS NOT NULL
    OR start_date_int IS NOT NULL
    OR placement_within_shofco_int IS NOT NULL