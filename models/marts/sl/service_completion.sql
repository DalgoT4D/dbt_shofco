{{ config(
    materialized='table',
    tags=['sl', 'service_completion', 'training_completion', 'feedback', 'ratings', 'followup', "sl_marts"]
) }}

SELECT
    case_id,
    id AS form_id,
    user_id AS enumerator_username,
    date_modified::timestamp AS submission_date,

    -- Participant Demographics
    NULLIF(TRIM(pp_fullname), '') AS participant_name,
    NULLIF(TRIM(pp_fullname), '') AS full_name, -- alias for compatibility
    NULLIF(TRIM(pp_unique_id), '') AS unique_id,
    CASE 
        WHEN age ~ '^[0-9]+$' THEN age::INT
        ELSE NULL
    END AS age,
    NULLIF(TRIM(sex), '') AS sex,
    NULLIF(TRIM(citizenship), '') AS citizenship,
    
    -- Date of birth - using coalesced field from staging
    CASE 
        WHEN date_of_birth ~ '^\d{4}-\d{2}-\d{2}' THEN date_of_birth::DATE
        ELSE NULL
    END AS date_of_birth,
    
    -- Contact and ID information
    NULLIF(TRIM(national_id_number_dir), '') AS id_number,
    NULLIF(TRIM(primary_phone_number), '') AS phone_number,
    
    -- Location information
    NULLIF(TRIM(county), '') AS county,
    NULLIF(TRIM(pp_shofco_subcounty), '') AS subcounty,
    NULLIF(TRIM(pp_ahofco_ward), '') AS ward,
    
    -- Enumerator information
    CONCAT(
        COALESCE(NULLIF(TRIM(enumerator_first_Name), ''), ''), 
        CASE 
            WHEN NULLIF(TRIM(enumerator_first_Name), '') IS NOT NULL 
                 AND NULLIF(TRIM(enumerator_last_name), '') IS NOT NULL 
            THEN ' ' 
            ELSE '' 
        END,
        COALESCE(NULLIF(TRIM(enumerator_last_name), ''), '')
    ) AS enumerator_full_name,
    
    -- PWD status
    CASE 
        WHEN LOWER(TRIM(is_pwd)) IN ('yes', 'true', '1') THEN 'Yes'
        WHEN LOWER(TRIM(is_pwd)) IN ('no', 'false', '0') THEN 'No'
        ELSE 'Unknown'
    END AS is_pwd,

    -- Training and completion information
    NULLIF(TRIM(training_activity), '') AS training_activity,
    NULLIF(TRIM(training_activity_tc), '') AS training_activity_tc,
    
    -- Training Completion Status
    CASE 
        WHEN LOWER(TRIM(completed_training_tc)) IN ('yes', 'completed', 'true', '1') THEN 'Completed'
        WHEN LOWER(TRIM(completed_training_tc)) IN ('no', 'not completed', 'false', '0') THEN 'Not Completed'
        ELSE NULLIF(TRIM(completed_training_tc), '')
    END AS completion_status,
    
    -- Training Feedback and Ratings
    CASE 
        WHEN LOWER(TRIM(how_helpful_course_tc)) IN ('very helpful', 'helpful', 'yes') THEN 'Helpful'
        WHEN LOWER(TRIM(how_helpful_course_tc)) IN ('not helpful', 'no') THEN 'Not Helpful'
        WHEN LOWER(TRIM(how_helpful_course_tc)) IN ('somewhat helpful', 'moderate') THEN 'Somewhat Helpful'
        ELSE NULLIF(TRIM(how_helpful_course_tc), '')
    END AS service_rating,
    
    CASE 
        WHEN LOWER(TRIM(confidence_to_find_employment_tc)) IN ('very confident', 'confident', 'yes') THEN 'Confident'
        WHEN LOWER(TRIM(confidence_to_find_employment_tc)) IN ('not confident', 'no') THEN 'Not Confident'
        WHEN LOWER(TRIM(confidence_to_find_employment_tc)) IN ('somewhat confident', 'moderate') THEN 'Somewhat Confident'
        ELSE NULLIF(TRIM(confidence_to_find_employment_tc), '')
    END AS employment_confidence,
    
    CASE 
        WHEN LOWER(TRIM(recommend_training)) IN ('yes', 'true', '1') THEN 'Yes'
        WHEN LOWER(TRIM(recommend_training)) IN ('no', 'false', '0') THEN 'No'
        ELSE NULLIF(TRIM(recommend_training), '')
    END AS would_recommend,
    
    -- Course Information
    NULLIF(TRIM(course_enrolled_sr), '') AS enrolled_course,
    NULLIF(TRIM(completed_courses), '') AS completed_courses,
    NULLIF(TRIM(dropped_courses), '') AS dropped_courses,
    NULL AS total_courses_completed,
    NULL AS total_courses_dropped,
    
    -- Registration date placeholder
    NULL AS registration_date,

    -- Derived fields for analytics
    CASE
        WHEN LOWER(TRIM(sex)) = 'male' THEN 'Male'
        WHEN LOWER(TRIM(sex)) = 'female' THEN 'Female'
        ELSE 'Other/Unknown'
    END AS gender_group,

    CASE
        WHEN LOWER(TRIM(citizenship)) IN ('refugee', 'non-kenyan', 'refugee/alien') THEN 'Refugee/Non-Kenyan'
        WHEN LOWER(TRIM(citizenship)) = 'kenyan' THEN 'Kenyan'
        ELSE 'Unknown'
    END AS refugee_status,

    CASE
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 18 AND 24 THEN '18-24'
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 25 AND 34 THEN '25-34'
        WHEN age ~ '^[0-9]+$' AND age::INT BETWEEN 35 AND 44 THEN '35-44'
        WHEN age ~ '^[0-9]+$' AND age::INT >= 45 THEN '45+'
        ELSE 'Unknown'
    END AS age_group

FROM {{ ref('staging_sl_case_table') }}
WHERE 
    -- Include records with participant names and training/completion data
    pp_fullname IS NOT NULL 
    AND (training_activity IS NOT NULL 
         OR training_activity_tc IS NOT NULL
         OR how_helpful_course_tc IS NOT NULL 
         OR confidence_to_find_employment_tc IS NOT NULL 
         OR recommend_training IS NOT NULL 
         OR completed_training_tc IS NOT NULL
         OR age IS NOT NULL 
         OR sex IS NOT NULL)