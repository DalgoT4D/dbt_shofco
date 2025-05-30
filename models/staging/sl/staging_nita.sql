{{ config(
  materialized='table',
  tags=['sl', 'nita']
) }}

WITH nita_data AS (
    SELECT
        id,
        -- Extract case details
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb -> 'form' ->> 'nita_exams' AS nita_exams,
        -- Extract received_on and submit_ip
        data::jsonb ->> 'received_on' AS received_on,
        data::jsonb ->> 'submit_ip' AS submit_ip,
        -- Handle boolean conversion for is_phone_submission and initial_processing_complete
        data::jsonb ->> 'is_phone_submission' AS is_phone_submission,
        data::jsonb ->> 'initial_processing_complete' AS initial_processing_complete,
        -- Extract metadata fields
        data::jsonb -> 'metadata' ->> 'username' AS metadata_username,
        -- Extract attachments
        data::jsonb -> 'attachments' -> 'form.xml' ->> 'length' AS attachments_form_xml_length
    FROM {{ source('staging_sl', 'NITA_Status') }}
    WHERE
        data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
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
    metadata_username,
    attachments_form_xml_length
FROM nita_data
