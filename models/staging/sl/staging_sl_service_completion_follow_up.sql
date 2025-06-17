{{ config(
    materialized='table',
    tags=['sl', 'service_completion']
) }}

WITH raw_data AS (
    SELECT
        data::jsonb AS form_data,
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS form_id,
        data::jsonb -> 'form' -> 'meta' ->> 'username' AS enumerator_username,
        data::jsonb ->> 'received_on' AS submission_date,
        data::jsonb -> 'form' ->> 'training_activity_tc' AS training_activity,
        data::jsonb -> 'form' ->> 'completed_training_tc' AS completed_training,
        data::jsonb -> 'form' -> 'if_completed_service' ->> 'how_helpful_course_tc' AS helpfulness_rating,
        data::jsonb -> 'form' -> 'if_completed_service' ->> 'recommend_training_tc' AS would_recommend,
        data::jsonb -> 'form' -> 'if_completed_service' ->> 'confidence_to_find_employment_tc' AS confident_to_find_employment,

        -- Participant demographic and metadata (now correctly from 'case' -> 'update')
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_fullname' AS participant_name,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_unique_id' AS unique_id,
        CASE
            WHEN data::jsonb -> 'form' -> 'meta_data' ->> 'pp_age' ~ '^\d+(\.\d+)?$' 
                THEN FLOOR((data::jsonb -> 'form' -> 'meta_data' ->> 'pp_age')::numeric)::int
            ELSE NULL
        END AS age,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_sex' AS sex,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_shofco_county' AS county,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_shofco_subcounty' AS subcounty,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_ahofco_ward' AS ward,

        -- Course data
        data::jsonb -> 'form' -> 'meta_data' ->> 'completed_courses' AS completed_courses,
        data::jsonb -> 'form' -> 'meta_data' ->> 'dropped_courses' AS dropped_courses,
        data::jsonb -> 'form' -> 'meta_data' ->> 'total_completed' AS total_courses_completed,
        data::jsonb -> 'form' -> 'meta_data' ->> 'total_dropped' AS total_courses_dropped

    FROM {{ source('staging_sl', 'Service_Completion_Follow_up') }}
    WHERE (data::jsonb ->> 'archived') IS NULL OR (data::jsonb ->> 'archived') = 'false'
)

SELECT
    form_id,
    case_id,
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
