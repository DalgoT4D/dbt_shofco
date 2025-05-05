{{
    config(
        materialized='table',
        schema=generate_schema_name('intermediate_sl', none),  
        alias='sl_mobilization_analytics',
        tags=['sl', 'mobilization', 'analytics']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_mobilization') }}
),

-- Prepare base data with all needed fields
enriched_data AS (
    SELECT
        case_id,
        fullname,
        first_name,
        middle_name,
        last_name_surname,
        gender,
        date_of_birth,
        national_id_number,
        phone_number,
        alternative_phone_number,
        is_plwd,
        is_refugee,
        level_of_education,
        date_of_registration,
        registered_by,
        county,
        county_shofco_site,
        sub_county,
        subcounty_shofco_site,
        ward,
        ward_shofco_site,
        village,
        service_of_interest,
        training_course,
        tvet_course_of_interest,
        vocational_courses_of_interest,
        referral,
        who_referred_the_participant,
        additional_comments
    FROM source
),

-- Calculate age based on date of birth
participant_with_age AS (
    SELECT
        *,
        CASE 
            WHEN date_of_birth IS NOT NULL 
            THEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM date_of_birth::date)
            ELSE NULL
        END AS age
    FROM enriched_data
),

-- Add derived categories
categorized_data AS (
    SELECT
        *,
        CASE 
            WHEN age < 18 THEN 'Under 18'
            WHEN age BETWEEN 18 AND 24 THEN '18-24'
            WHEN age BETWEEN 25 AND 34 THEN '25-34'
            WHEN age BETWEEN 35 AND 44 THEN '35-44'
            WHEN age >= 45 THEN '45+'
            ELSE 'Unknown'
        END AS age_category,
        
        CASE
            WHEN level_of_education IN ('Primary', 'primary') THEN 'Primary'
            WHEN level_of_education IN ('Secondary', 'secondary') THEN 'Secondary'
            WHEN level_of_education IN ('Tertiary', 'tertiary', 'College', 'college', 'University', 'university') THEN 'Tertiary'
            WHEN level_of_education IN ('None', 'none', 'No Education', 'no education') THEN 'No Formal Education'
            ELSE 'Other/Unknown'
        END AS education_level_standardized
    FROM participant_with_age
)

-- Final output table
SELECT * FROM categorized_data