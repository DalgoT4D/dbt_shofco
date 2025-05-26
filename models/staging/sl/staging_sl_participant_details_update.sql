{{ config(
    materialized='table',
    tags=['sl', 'participant_update']
) }}

WITH raw_data AS (
    SELECT
        data::jsonb AS form_data,

        -- Metadata
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS form_id,
        data::jsonb -> 'form' -> 'meta' ->> 'username' AS submitted_by,
        data::jsonb -> 'form' -> 'meta' ->> 'userID' AS user_id,
        data::jsonb -> 'form' -> 'meta' ->> 'deviceID' AS device_id,
        data::jsonb -> 'form' -> 'meta' ->> 'timeStart' AS time_start,
        data::jsonb -> 'form' -> 'meta' ->> 'timeEnd' AS time_end,
        data::jsonb -> 'form' -> 'meta' ->> 'appVersion' AS app_version,
        data::jsonb -> 'form' -> 'meta' ->> 'commcare_version' AS commcare_version,
        data::jsonb -> 'form' -> 'meta' ->> 'app_build_version' AS app_build_version,

        -- Form attributes
        data::jsonb -> 'form' ->> '@name' AS form_name,
        data::jsonb -> 'form' ->> '@version' AS form_version,

        -- Participant info
        data::jsonb -> 'form' ->> 'name' AS participant_name,

        -- Case info
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS case_user_id,
        data::jsonb -> 'form' -> 'case' ->> '@date_modified' AS date_modified,

        -- Submission tracking
        data::jsonb ->> 'received_on' AS received_on,
        data::jsonb ->> 'server_modified_on' AS server_modified_on,
        data::jsonb ->> 'indexed_on' AS indexed_on

    FROM {{ source('staging_sl', 'Participant_Details_Update') }}
    WHERE (data::jsonb ->> 'archived') IS NULL OR (data::jsonb ->> 'archived') = 'false'
)

SELECT
    form_id,
    submitted_by,
    user_id,
    device_id,
    time_start::timestamp AS time_start,
    time_end::timestamp AS time_end,
    app_version,
    commcare_version,
    app_build_version,
    form_name,
    form_version,

    participant_name,

    case_id,
    case_user_id,
    date_modified::timestamp AS date_modified,

    received_on::timestamp,
    server_modified_on::timestamp,
    indexed_on::timestamp

FROM raw_data
