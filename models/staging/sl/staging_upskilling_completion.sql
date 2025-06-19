{{
    config(
        materialized="table", 
        unique_key="case_id", 
        tags=["upskilling", "upskilling_completion", "sl"]
    )
}}

WITH source_data AS (
    SELECT
        id,
        data
    FROM {{ source('staging_sl', 'upskilling_completion') }}
)

SELECT
    -- Case identifiers
    data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
    data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,

    -- Enumerator details (correct source is `meta_data`)
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'enumerator', '') AS enumerator,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'enumerators_first', '') AS enumerator_first_name,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'enumerator_last-name', '') AS enumerator_last_name,

    -- Completion status (top-level under form)
    NULLIF(data::jsonb -> 'form' ->> 'completed_upskilling_uf', '') AS completed_upskilling,
    NULLIF(data::jsonb -> 'form' ->> 'how_helpful_was_upskilling_uc', '') AS how_helpful_was_upskilling

FROM source_data