{{ config(
  materialized='table',
  tags=["service_completion_followup", "sl"]
) }}

SELECT
    NULLIF(form_id, '') AS form_id,
    NULLIF(case_id, '') AS case_id,
    NULLIF(enumerator_username, '') AS enumerator_username,
    NULLIF(submission_date::timestamp, '') AS submission_date,

    -- Training feedback
    NULLIF(training_activity, '') AS training_activity,
    NULLIF(completed_training, '') AS completed_training,
    NULLIF(helpfulness_rating, '') AS helpfulness_rating,
    NULLIF(would_recommend, '') AS would_recommend,
    NULLIF(confident_to_find_employment, '') AS confident_to_find_employment,

    -- Participant info
    NULLIF(participant_name, '') AS participant_name,
    NULLIF(unique_id, '') AS unique_id,
    NULLIF(age, '') AS age,
    NULLIF(sex, '') AS sex,
    NULLIF(county, '') AS county,
    NULLIF(subcounty, '') AS subcounty,
    NULLIF(ward, '') AS ward,

    -- Course summary
    NULLIF(completed_courses, '') AS completed_courses,
    NULLIF(dropped_courses, '') AS dropped_courses,
    NULLIF(total_courses_completed, '') AS total_courses_completed,
    NULLIF(total_courses_dropped, '') AS total_courses_dropped

FROM {{ ref("staging_sl_service_completion_follow_up") }}