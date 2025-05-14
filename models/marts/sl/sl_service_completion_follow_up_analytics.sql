{{ config(
    materialized='table',
    tags=['sustainable_livelihoods', 'service_completion']
) }}

WITH base AS (
    SELECT
        form_id,
        enumerator_username,
        submission_date,
        training_activity,
        completed_training,
        helpfulness_rating,
        would_recommend,
        confident_to_find_employment,

        participant_name,
        unique_id,
        age,
        sex,
        county,
        subcounty,
        ward,

        completed_courses,
        dropped_courses,
        total_courses_completed,
        total_courses_dropped

    FROM {{ ref('staging_sl_service_completion_follow_up') }}
)

SELECT *
FROM base
