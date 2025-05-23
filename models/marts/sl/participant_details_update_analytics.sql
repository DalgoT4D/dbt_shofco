{{ config(
    materialized='table',
    unique_key='case_id',
    alias='participant_details_update_analytics',
    tags=['sustainable_livelihoods', 'participant_update', 'analytics']
) }}

WITH base AS (
    SELECT
        form_id,
        submitted_by,
        user_id,
        device_id,
        time_start::date AS time_start,
        time_end::date AS time_end,
        app_version,
        commcare_version,
        app_build_version,
        form_name,
        form_version,
        participant_name,
        case_id,
        case_user_id,
        date_modified::date AS date_modified,
        received_on::date AS received_on,
        server_modified_on::date AS server_modified_on,
        indexed_on::date AS indexed_on,

        -- Derived fields
        received_on::date AS submission_day,
        date_trunc('month', received_on)::date AS submission_month,
        EXTRACT(EPOCH FROM (time_end - time_start)) / 60.0 AS duration_minutes

    FROM {{ ref('staging_sl_participant_details_update') }}
)

SELECT *
FROM base
