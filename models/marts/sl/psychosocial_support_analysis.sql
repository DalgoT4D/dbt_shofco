{{ config(
    materialized='table',
    tags=['sl', 'psychosocial', 'support', 'swep', 'group_sessions', 'meetings', "sl_marts"]
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
    pp_shofco_subcounty AS subcounty,
    
    -- Psychosocial Support Group Information
    NULLIF(TRIM(group_psr), '') AS support_group,
    NULLIF(TRIM(other_group_psr), '') AS other_group_details,
    
    -- SWEP Group Classification
    CASE 
        WHEN LOWER(TRIM(group_psr)) LIKE '%swep%' THEN 'SWEP Group'
        WHEN LOWER(TRIM(other_group_psr)) LIKE '%swep%' THEN 'SWEP Group'
        ELSE 'Non-SWEP Group'
    END AS group_type,
    
    -- Session Attendance
    CASE 
        WHEN people_attending_psr ~ '^[0-9]+$' THEN people_attending_psr::INT
        ELSE NULL
    END AS session_attendance_count,
    
    -- Meeting Details (Enhanced from both models)
    NULLIF(TRIM(meeting_topic_psr), '') AS meeting_topic,
    {{ validate_date('meeting_date_psr') }} AS meeting_date,
    NULLIF(TRIM(meeting_county_psr), '') AS meeting_county,
    NULLIF(TRIM(meeting_subcounty_psr), '') AS meeting_subcounty,
    NULLIF(TRIM(meeting_ward_psr), '') AS meeting_ward,
    
    -- Session Management (from psychosocial_support model)
    CASE 
        WHEN indexed_on ~ '^\d{4}-\d{2}-\d{2}' THEN indexed_on::DATE
        ELSE NULL
    END AS received_on,
    
    -- Facilitator Information
    COALESCE(
        NULLIF(TRIM(psychosocial_facilitator_psr), ''),
        NULLIF(TRIM(name_of_facilitator), '')
    ) AS facilitator_name,
    NULLIF(TRIM(profession_of_facilitator_psr), '') AS facilitator_profession,
    NULLIF(TRIM(officer_name_psr), '') AS officer_name,
    
    -- Enumerator Information
    CONCAT(COALESCE(enumerator_first_name, ''), ' ', COALESCE(enumerator_last_name, '')) AS enumerator_full_name,
    enumerator_first_name AS enumerator_first,
    enumerator_last_name AS enumerator_last,
    enumerator AS username,
    
    -- Program Information
    NULLIF(TRIM(program_psr), '') AS program_type,
    NULLIF(TRIM(specify_other_psr), '') AS other_program_details,
    
    -- Course Enrollment (for SWEP connection analysis)
    NULLIF(TRIM(course_enrolled_sr), '') AS enrolled_course,
    
    -- SWEP Beneficiary Flag (based on course enrollment)
    CASE 
        WHEN LOWER(TRIM(course_enrolled_sr)) LIKE '%swep%' 
             OR LOWER(TRIM(course_enrolled_sr)) LIKE '%tailoring%'
             OR LOWER(TRIM(course_enrolled_sr)) LIKE '%iga%'
             OR LOWER(TRIM(course_enrolled_sr)) LIKE '%handicraft%' THEN 'SWEP Beneficiary'
        ELSE 'Non-SWEP Beneficiary'
    END AS swep_beneficiary_status,
    
    -- Analysis: SWEP Beneficiaries in Psychosocial Support
    CASE 
        WHEN (LOWER(TRIM(course_enrolled_sr)) LIKE '%swep%' 
              OR LOWER(TRIM(course_enrolled_sr)) LIKE '%tailoring%'
              OR LOWER(TRIM(course_enrolled_sr)) LIKE '%iga%'
              OR LOWER(TRIM(course_enrolled_sr)) LIKE '%handicraft%')
             AND (LOWER(TRIM(group_psr)) LIKE '%swep%' 
                  OR LOWER(TRIM(other_group_psr)) LIKE '%swep%') THEN 'SWEP_Beneficiary_In_SWEP_PSS'
        WHEN (LOWER(TRIM(course_enrolled_sr)) LIKE '%swep%' 
              OR LOWER(TRIM(course_enrolled_sr)) LIKE '%tailoring%'
              OR LOWER(TRIM(course_enrolled_sr)) LIKE '%iga%'
              OR LOWER(TRIM(course_enrolled_sr)) LIKE '%handicraft%')
             AND NOT (LOWER(TRIM(group_psr)) LIKE '%swep%' 
                     OR LOWER(TRIM(other_group_psr)) LIKE '%swep%') THEN 'SWEP_Beneficiary_In_Other_PSS'
        ELSE 'Other'
    END AS swep_pss_classification,
    
    -- Refugee Status for analysis
    CASE
        WHEN LOWER(TRIM(citizenship)) IN ('refugee', 'non-kenyan', 'refugee/alien') THEN 'Refugee/Non-Kenyan'
        WHEN LOWER(TRIM(citizenship)) = 'kenyan' THEN 'Kenyan'
        ELSE 'Unknown'
    END AS refugee_status,
    
    -- SWEP Session Analysis (from psychosocial_support model)
    CASE
        WHEN LOWER(group_psr) LIKE '%swep%' OR LOWER(other_group_psr) LIKE '%swep%' THEN TRUE
        ELSE FALSE
    END AS is_swep_session,
    
    -- Session processing flag (legacy compatibility)
    NULL AS initial_processing_complete,
    
    -- Metadata
    indexed_on AS date_received,
    date_modified AS last_modified

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    -- Include records with psychosocial support information
    group_psr IS NOT NULL 
    OR other_group_psr IS NOT NULL
    OR people_attending_psr IS NOT NULL
    OR meeting_topic_psr IS NOT NULL
    OR psychosocial_facilitator_psr IS NOT NULL