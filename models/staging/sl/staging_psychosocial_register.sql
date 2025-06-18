{{ config(
  materialized='table',
  tags=['sl', 'psychosocial']
) }}

WITH psycho_data AS (
    SELECT
        id,
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb ->> 'received_on' AS received_on,
        data::jsonb ->> 'initial_processing_complete' AS initial_processing_complete,
        data::jsonb -> 'form' ->> 'meeting_date_psr' AS meeting_date,
        data::jsonb -> 'form' ->> 'meeting_topic_psr' AS meeting_topic,
        data::jsonb -> 'form' ->> 'group_psr' AS group_name,
        data::jsonb -> 'form' ->> 'enumerator' AS enumerator_full_name,
        data::jsonb -> 'form' ->> 'enumerators_first' AS enumerator_first,
        data::jsonb -> 'form' ->> 'enumerator_last-name' AS enumerator_last,
        data::jsonb -> 'form' ->> 'people_attending_psr' AS people_attending,
        data::jsonb -> 'form' ->> 'psychosocial_facilitator_psr' AS facilitator_name,
        data::jsonb -> 'form' ->> 'profession_of_facilitator_psr' AS facilitator_profession,
        data::jsonb -> 'form' -> 'shofco_site_details' ->> 'meeting_ward_psr' AS ward,
        data::jsonb -> 'form' -> 'shofco_site_details' ->> 'meeting_subcounty_psr' AS subcounty,
        data::jsonb -> 'form' -> 'shofco_site_details' ->> 'meeting_county_psr' AS county,
        data::jsonb -> 'metadata' ->> 'username' AS username
    FROM {{ source('staging_sl', 'Psychosocial_Support_Register') }}
    WHERE data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
)

SELECT DISTINCT
    id,
    case_id,
    user_id,
    received_on,
    initial_processing_complete,
    meeting_date,
    meeting_topic,
    group_name,
    enumerator_full_name,
    enumerator_first,
    enumerator_last,
    people_attending,
    facilitator_name,
    facilitator_profession,
    ward,
    subcounty,
    county,
    username
FROM psycho_data
