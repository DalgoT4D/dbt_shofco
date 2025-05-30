{{ config(
  materialized='table',
  tags=['dignity_kit_issuance', 'sl']
) }}

WITH dignity_kit_data AS (
    SELECT
        id,
        -- Extract case details
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator' AS enumerator,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'intern_name' AS intern_name,
        -- Refactored fields
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_of_issuance_dki' AS date_of_issuance,
        STRING_AGG(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'dignity_received_dki', ', ') AS dignity_received,
        -- Extract participant details
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_age' AS age,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_sex' AS sex,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_fullname' AS fullname,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_unique_id' AS participant_id,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_shofco_county' AS county,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_shofco_subcounty' AS subcounty,
        data::jsonb -> 'form' -> 'meta_data' ->> 'pp_ahofco_ward' AS ward,
        -- Extract submission details
        data::jsonb ->> 'received_on' AS date_received
    FROM {{ source('staging_sl', 'Dignity_Kit_Issuance') }}
    WHERE data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
    GROUP BY id, case_id, user_id, enumerator, intern_name, date_of_issuance, age, sex, fullname, 
             participant_id, county, subcounty, ward, date_received
)

SELECT DISTINCT
    id,
    case_id,
    user_id,
    enumerator,
    intern_name,
    date_of_issuance,
    dignity_received,
    age,
    sex,
    fullname,
    participant_id,
    county,
    subcounty,
    ward,
    date_received
FROM dignity_kit_data
