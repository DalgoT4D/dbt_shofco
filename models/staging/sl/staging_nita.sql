{{ config(
  materialized='table',
  tags=['nita_exams_followup', 'sl', 'nita']
) }}

WITH nita_data AS (
    SELECT
        id,
        -- Ensure case_id is always available
        COALESCE(data::jsonb -> 'form' -> 'case' ->> '@case_id', 'Unknown') AS case_id,
        -- Provide a default value for user_id if missing
        COALESCE(data::jsonb -> 'form' -> 'case' ->> '@user_id', 'No User') AS user_id,
        -- Use COALESCE for nita_exams field, defaulting to 'Not Provided'
        COALESCE(data::jsonb -> 'form' ->> 'nita_exams', 'Not Provided') AS nita_exams,
        -- Convert received_on to a proper date format with a fallback
        COALESCE(NULLIF(data::jsonb ->> 'received_on', ''), CURRENT_DATE::TEXT) AS received_on,
        -- Provide a fallback IP if submit_ip is missing
        COALESCE(data::jsonb ->> 'submit_ip', 'Unknown') AS submit_ip,
        -- Handle boolean conversion for is_phone_submission
        COALESCE(NULLIF(data::jsonb ->> 'is_phone_submission', ''), 'false')::BOOLEAN AS is_phone_submission,
        -- Handle initial processing complete flag
        COALESCE(NULLIF(data::jsonb ->> 'initial_processing_complete', ''), 'false')::BOOLEAN AS initial_processing_complete,
        -- Ensure archived field is properly handled
        COALESCE(NULLIF(data::jsonb ->> 'archived', ''), 'false')::BOOLEAN AS archived,
        -- Provide a default domain if missing
        COALESCE(data::jsonb ->> 'domain', 'No Domain') AS domain,
        -- Ensure metadata_username is present
        COALESCE(data::jsonb -> 'metadata' ->> 'username', 'Guest') AS metadata_username,
        -- Handle missing numeric values using COALESCE with default zero
        COALESCE(NULLIF(data::jsonb -> 'attachments' -> 'form.xml' ->> 'length', ''), '0')::INTEGER AS attachments_form_xml_length
    FROM {{ source('staging_sl', 'NITA_Status') }}
    WHERE
        COALESCE(NULLIF(data::jsonb ->> 'archived', ''), 'false') = 'false'
)

SELECT DISTINCT
    id,
    case_id,
    user_id,
    nita_exams,
    received_on,
    submit_ip,
    is_phone_submission,
    initial_processing_complete,
    archived,
    domain,
    metadata_username,
    attachments_form_xml_length
FROM nita_data
