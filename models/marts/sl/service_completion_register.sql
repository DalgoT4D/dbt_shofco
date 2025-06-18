{{ config(
    materialized='table',
    tags=['sl', 'training_completion', 'demographics']
) }}

WITH register AS (
    SELECT
        case_id,
        NULLIF(full_name, '') AS full_name,

        -- Safe integer casting for age using regex and type casting
        CASE 
            WHEN age::TEXT ~ '^\d+$' THEN age::INT
            ELSE NULL
        END AS age,

        NULLIF(sex, '') AS sex,
        NULLIF(citizenship, '') AS citizenship,

        -- Safe date casting
        CASE 
            WHEN date_of_birth::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN date_of_birth::DATE
            ELSE NULL
        END AS date_of_birth,
        CASE 
            WHEN registration_date::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN registration_date::DATE
            ELSE NULL
        END AS registration_date,

        NULLIF(id_number, '') AS id_number,
        NULLIF(phone_number, '') AS phone_number,
        NULLIF(enumerator_full_name, '') AS enumerator_full_name,
        NULLIF(county, '') AS county,
        NULLIF(subcounty, '') AS subcounty,
        NULLIF(ward, '') AS ward
    FROM {{ ref('staging_register') }}
),

service_completion AS (
    SELECT
        case_id,
        NULLIF(completed_training, '') AS completed_training,

        CASE 
            WHEN submission_date::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN submission_date::DATE
            ELSE NULL
        END AS submission_date,

        NULLIF(completed_courses, '') AS completed_courses,
        NULLIF(dropped_courses, '') AS dropped_courses,
        NULLIF(total_courses_completed, '') AS total_courses_completed,
        NULLIF(total_courses_dropped, '') AS total_courses_dropped,
        NULLIF(training_activity, '') AS training_activity,
        NULLIF(helpfulness_rating, '') AS helpfulness_rating,
        NULLIF(would_recommend, '') AS would_recommend,
        NULLIF(confident_to_find_employment, '') AS confident_to_find_employment
    FROM {{ ref('staging_sl_service_completion_follow_up') }}
    WHERE completed_training ILIKE 'yes'
)

SELECT
    r.case_id,
    r.full_name,
    r.age,
    r.sex,
    r.citizenship,
    r.date_of_birth,
    r.registration_date,
    r.id_number,
    r.phone_number,
    r.enumerator_full_name,
    r.county,
    r.subcounty,
    r.ward,
    sc.completed_training,
    sc.submission_date,
    sc.completed_courses,
    sc.dropped_courses,
    sc.total_courses_completed,
    sc.total_courses_dropped,
    sc.training_activity,
    sc.helpfulness_rating,
    sc.would_recommend,
    sc.confident_to_find_employment,

    -- Gender group
    CASE
        WHEN LOWER(r.sex) = 'male' THEN 'Male'
        WHEN LOWER(r.sex) = 'female' THEN 'Female'
        ELSE 'Other/Unknown'
    END AS gender_group,

    -- Refugee status
    CASE
        WHEN LOWER(r.citizenship) IN ('refugee', 'non-kenyan', 'refugee/alien') THEN 'Refugee/Non-Kenyan'
        WHEN LOWER(r.citizenship) = 'kenyan' THEN 'Kenyan'
        ELSE 'Unknown'
    END AS refugee_status,

    -- Placeholder for PWD status
    NULL AS is_pwd,

    -- Age group buckets
    CASE
        WHEN r.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN r.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN r.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN r.age >= 45 THEN '45+'
        ELSE 'Unknown'
    END AS age_group

FROM register r
LEFT JOIN service_completion sc ON r.case_id = sc.case_id
WHERE sc.case_id IS NOT NULL
