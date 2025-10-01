{{ config(
    materialized='table',
    tags=['sl', 'apprenticeship', 'skills', 'placement', "sl_marts"]
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
    
    -- Apprenticeship Skills and Details
    NULLIF(TRIM(skill_enrolled_apr), '') AS enrolled_skill,
    NULLIF(TRIM(other_skill_apr), '') AS other_skill_details,
    
    -- Provider Information
    NULLIF(TRIM(apprenticeship_provider_apr), '') AS provider_name,
    NULLIF(TRIM(official_name_of_the_apprentice_provider_apbl), '') AS official_provider_name,
    NULLIF(TRIM(apprenticeship_provider_contact_apr), '') AS provider_contact,
    NULLIF(TRIM(official_contact_of_the_apprentice_provider_apbl), '') AS official_provider_contact,
    
    -- Provider Location
    NULLIF(TRIM(county_of_apprenticeship_provider_apr), '') AS provider_county,
    NULLIF(TRIM(subcounty_apprenticeship_provider_apr), '') AS provider_subcounty,
    NULLIF(TRIM(ward_apprenticeship_provider_apr), '') AS provider_ward,
    
    -- Apprenticeship Location
    NULLIF(TRIM(county_of_apprenticeship_apr), '') AS apprenticeship_county,
    
    -- Dates and Duration
    {{ validate_date('placement_date_apr') }} AS placement_date,
    {{ validate_date('completion_date_apr') }} AS completion_date,
    
    -- Calculate apprenticeship duration
    CASE 
        WHEN {{ validate_date('placement_date_apr') }} IS NOT NULL 
             AND {{ validate_date('completion_date_apr') }} IS NOT NULL
        THEN {{ validate_date('completion_date_apr') }} - {{ validate_date('placement_date_apr') }}
        ELSE NULL
    END AS apprenticeship_duration_days,
    
    -- Guardian Information
    NULLIF(TRIM(guardians_name_apr), '') AS guardian_name,
    NULLIF(TRIM(guardians_phone_number_apr), '') AS guardian_phone,
    NULLIF(TRIM(guardians_alternative_number_apr), '') AS guardian_alt_phone,
    
    -- Skills Classification
    CASE 
        WHEN LOWER(TRIM(skill_enrolled_apr)) LIKE '%tailoring%' 
             OR LOWER(TRIM(skill_enrolled_apr)) LIKE '%sewing%' THEN 'Tailoring/Sewing'
        WHEN LOWER(TRIM(skill_enrolled_apr)) LIKE '%carpentry%' 
             OR LOWER(TRIM(skill_enrolled_apr)) LIKE '%construction%' THEN 'Carpentry/Construction'
        WHEN LOWER(TRIM(skill_enrolled_apr)) LIKE '%plumbing%' THEN 'Plumbing'
        WHEN LOWER(TRIM(skill_enrolled_apr)) LIKE '%electrical%' 
             OR LOWER(TRIM(skill_enrolled_apr)) LIKE '%electronics%' THEN 'Electrical/Electronics'
        WHEN LOWER(TRIM(skill_enrolled_apr)) LIKE '%welding%' 
             OR LOWER(TRIM(skill_enrolled_apr)) LIKE '%metal%' THEN 'Welding/Metalwork'
        WHEN LOWER(TRIM(skill_enrolled_apr)) LIKE '%beauty%' 
             OR LOWER(TRIM(skill_enrolled_apr)) LIKE '%salon%' THEN 'Beauty/Salon Services'
        WHEN LOWER(TRIM(skill_enrolled_apr)) LIKE '%automotive%' 
             OR LOWER(TRIM(skill_enrolled_apr)) LIKE '%mechanic%' THEN 'Automotive/Mechanical'
        WHEN LOWER(TRIM(skill_enrolled_apr)) LIKE '%catering%' 
             OR LOWER(TRIM(skill_enrolled_apr)) LIKE '%food%' THEN 'Catering/Food Services'
        ELSE COALESCE(NULLIF(TRIM(skill_enrolled_apr), ''), 'Other/Unspecified')
    END AS skill_category,
    
    -- Duration Categories
    CASE 
        WHEN {{ validate_date('placement_date_apr') }} IS NOT NULL 
             AND {{ validate_date('completion_date_apr') }} IS NOT NULL THEN
            CASE 
                WHEN ({{ validate_date('completion_date_apr') }} - {{ validate_date('placement_date_apr') }}) <= 90 THEN '3 Months or Less'
                WHEN ({{ validate_date('completion_date_apr') }} - {{ validate_date('placement_date_apr') }}) <= 180 THEN '3-6 Months'
                WHEN ({{ validate_date('completion_date_apr') }} - {{ validate_date('placement_date_apr') }}) <= 365 THEN '6 Months - 1 Year'
                WHEN ({{ validate_date('completion_date_apr') }} - {{ validate_date('placement_date_apr') }}) <= 730 THEN '1-2 Years'
                ELSE 'More than 2 Years'
            END
        ELSE 'Duration Unknown'
    END AS duration_category,
    
    -- Completion Status
    CASE 
        WHEN {{ validate_date('completion_date_apr') }} IS NOT NULL THEN 'Completed'
        WHEN {{ validate_date('placement_date_apr') }} IS NOT NULL 
             AND {{ validate_date('completion_date_apr') }} IS NULL THEN 'In Progress/Incomplete'
        ELSE 'Status Unknown'
    END AS completion_status,
    
    -- Age Groups
    CASE
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 16 AND 20 THEN '16-20'
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 21 AND 25 THEN '21-25'
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 26 AND 30 THEN '26-30'
        WHEN age ~ '^[0-9]+$' AND age::INT > 30 THEN '30+'
        ELSE 'Unknown'
    END AS age_group,
    
    -- Refugee Status
    CASE
        WHEN LOWER(TRIM(citizenship)) IN ('refugee', 'non-kenyan', 'refugee/alien') THEN 'Refugee/Non-Kenyan'
        WHEN LOWER(TRIM(citizenship)) = 'kenyan' THEN 'Kenyan'
        ELSE 'Unknown'
    END AS refugee_status,
    
    -- Provider Type (simplified)
    CASE 
        WHEN apprenticeship_provider_apr IS NOT NULL 
             AND official_name_of_the_apprentice_provider_apbl IS NOT NULL THEN 'Formal Provider'
        WHEN apprenticeship_provider_apr IS NOT NULL THEN 'Informal Provider'
        ELSE 'Unknown Provider Type'
    END AS provider_type,
    
    -- Cross-county placement (apprenticeship in different county than residence)
    CASE 
        WHEN county IS NOT NULL 
             AND county_of_apprenticeship_apr IS NOT NULL 
             AND TRIM(county) != TRIM(county_of_apprenticeship_apr) THEN 'Cross-County Placement'
        WHEN county IS NOT NULL 
             AND county_of_apprenticeship_apr IS NOT NULL 
             AND TRIM(county) = TRIM(county_of_apprenticeship_apr) THEN 'Same County Placement'
        ELSE 'Location Unknown'
    END AS placement_location_type,
    
    -- Metadata
    indexed_on AS date_received,
    date_modified AS last_modified

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    -- Include apprenticeship records
    skill_enrolled_apr IS NOT NULL 
    OR apprenticeship_provider_apr IS NOT NULL
    OR placement_date_apr IS NOT NULL
    OR completion_date_apr IS NOT NULL