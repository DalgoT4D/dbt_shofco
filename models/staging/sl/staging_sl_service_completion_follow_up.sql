{{ config(
    materialized='table',
    tags=['sl', 'service_completion']
) }}

WITH raw_data AS (
    SELECT
        data::jsonb AS form_data,
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS form_id,
        data::jsonb -> 'form' -> 'meta' ->> 'username' AS enumerator_username,
        data::jsonb ->> 'received_on' AS submission_date,
        data::jsonb -> 'form' ->> 'training_activity_tc' AS training_activity,
        data::jsonb -> 'form' ->> 'completed_training_tc' AS completed_training,
        data::jsonb -> 'form' -> 'if_completed_service' ->> 'how_helpful_course_tc' AS helpfulness_rating,
        data::jsonb -> 'form' -> 'if_completed_service' ->> 'recommend_training_tc' AS would_recommend,
        data::jsonb -> 'form' -> 'if_completed_service' ->> 'confidence_to_find_employment_tc' AS confident_to_find_employment,

        -- Participant demographic and metadata (now correctly from 'case' -> 'update')
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_fullname' AS participant_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_unique_id' AS unique_id,

        -- ✅ Safe casting for age (handles floats & text)
        CASE
            WHEN NULLIF(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age', '') IS NOT NULL
                 AND (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age') ~ '^[0-9]*\.?[0-9]+$'
            THEN FLOOR((data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age')::FLOAT)::INT
            ELSE NULL
        END AS age,

        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_sex' AS sex,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_county' AS county,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_subcounty' AS subcounty,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_ahofco_ward' AS ward,

        -- Course data
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'completed_courses' AS completed_courses,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'dropped_courses' AS dropped_courses,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'total_completed' AS total_courses_completed,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'total_dropped' AS total_courses_dropped

    FROM {{ source('staging_sl', 'Service_Completion_Follow_up') }}
    WHERE (data::jsonb ->> 'archived') IS NULL OR (data::jsonb ->> 'archived') = 'false'
)

SELECT
    form_id,
    enumerator_username,
    submission_date::timestamp AS submission_date,
    training_activity,
    completed_training,
    helpfulness_rating,
    would_recommend,
    confident_to_find_employment,

    -- Participant info
    participant_name,
    unique_id,
    age,
    sex,
    county,
    subcounty,
    ward,

    -- Course details
    completed_courses,
    dropped_courses,
    total_courses_completed,
    total_courses_dropped

FROM raw_data
