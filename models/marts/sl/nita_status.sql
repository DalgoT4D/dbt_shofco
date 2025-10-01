{{ config(
    materialized='table',
    tags=['sl', 'nita', 'exams', 'tvet', 'swep', "sl_marts"]
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
    
    -- Program/Course Information
    NULLIF(TRIM(course_enrolled_sr), '') AS enrolled_course,
    NULLIF(TRIM(course_enrolled_tvet), '') AS tvet_course,
    NULLIF(TRIM(training_program_uf), '') AS training_program,
    
    -- Program Type Classification
    CASE 
        WHEN LOWER(TRIM(course_enrolled_sr)) LIKE '%tvet%' 
             OR LOWER(TRIM(course_enrolled_tvet)) IS NOT NULL THEN 'TVET Vocational'
        WHEN LOWER(TRIM(course_enrolled_sr)) LIKE '%swep tailoring%' 
             OR LOWER(TRIM(training_program_uf)) LIKE '%tailoring%' THEN 'SWEP Tailoring'
        WHEN LOWER(TRIM(course_enrolled_sr)) LIKE '%swep%' THEN 'SWEP Other'
        ELSE 'Other Program'
    END AS program_type,
    
    -- NITA Exam Status
    CASE 
        WHEN LOWER(TRIM(nita_exams)) IN ('yes', 'true', '1') THEN 'Yes'
        WHEN LOWER(TRIM(nita_exams)) IN ('no', 'false', '0') THEN 'No'
        ELSE NULLIF(TRIM(nita_exams), '')
    END AS taking_nita_exams,
    
    CASE 
        WHEN LOWER(TRIM(nita_exams_tc)) IN ('yes', 'true', '1') THEN 'Yes'
        WHEN LOWER(TRIM(nita_exams_tc)) IN ('no', 'false', '0') THEN 'No'
        ELSE NULLIF(TRIM(nita_exams_tc), '')
    END AS nita_exams_tc_status,
    
    CASE 
        WHEN LOWER(TRIM(no_nita_exams_tc)) IN ('yes', 'true', '1') THEN 'Yes'
        WHEN LOWER(TRIM(no_nita_exams_tc)) IN ('no', 'false', '0') THEN 'No'
        ELSE NULLIF(TRIM(no_nita_exams_tc), '')
    END AS not_taking_nita_exams,
    
    -- Institution Information
    NULLIF(TRIM(name_of_institution_tvet), '') AS tvet_institution,
    CASE 
        WHEN LOWER(TRIM(offering_nita_exams_ttia)) IN ('yes', 'true', '1') THEN 'Yes'
        WHEN LOWER(TRIM(offering_nita_exams_ttia)) IN ('no', 'false', '0') THEN 'No'
        ELSE NULLIF(TRIM(offering_nita_exams_ttia), '')
    END AS institution_offers_nita,
    
    -- Completion Information
    CASE 
        WHEN LOWER(TRIM(completion_tvet)) IN ('yes', 'completed', 'true', '1') THEN 'Completed'
        WHEN LOWER(TRIM(completion_tvet)) IN ('no', 'not completed', 'false', '0') THEN 'Not Completed'
        ELSE NULLIF(TRIM(completion_tvet), '')
    END AS tvet_completion_status,
    
    -- Dates
    {{ validate_date('start_date_tvet') }} AS tvet_start_date,
    {{ validate_date('completion_date_tvet') }} AS tvet_completion_date,
    
    -- Analysis Flag: NITA Eligibility by Program
    CASE 
        WHEN (LOWER(TRIM(course_enrolled_sr)) LIKE '%tvet%' 
              OR LOWER(TRIM(course_enrolled_tvet)) IS NOT NULL)
             AND LOWER(TRIM(nita_exams)) = 'yes' THEN 'TVET_Taking_NITA'
        WHEN LOWER(TRIM(course_enrolled_sr)) LIKE '%swep tailoring%' 
             AND LOWER(TRIM(nita_exams)) = 'yes' THEN 'SWEP_Tailoring_Taking_NITA'
        WHEN (LOWER(TRIM(course_enrolled_sr)) LIKE '%tvet%' 
              OR LOWER(TRIM(course_enrolled_tvet)) IS NOT NULL)
             AND LOWER(TRIM(nita_exams)) = 'no' THEN 'TVET_Not_Taking_NITA'
        WHEN LOWER(TRIM(course_enrolled_sr)) LIKE '%swep tailoring%' 
             AND LOWER(TRIM(nita_exams)) = 'no' THEN 'SWEP_Tailoring_Not_Taking_NITA'
        ELSE 'Other'
    END AS nita_program_classification,
    
    -- Metadata
    indexed_on AS date_received,
    date_modified AS last_modified

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    -- Include records with NITA exam information or relevant program enrollment
    nita_exams IS NOT NULL 
    OR nita_exams_tc IS NOT NULL 
    OR no_nita_exams_tc IS NOT NULL
    OR course_enrolled_tvet IS NOT NULL
    OR (course_enrolled_sr IS NOT NULL AND (
        LOWER(course_enrolled_sr) LIKE '%tvet%' 
        OR LOWER(course_enrolled_sr) LIKE '%swep%'
    ))