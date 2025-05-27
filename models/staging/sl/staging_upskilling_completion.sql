{{
    config(
        materialized="table", 
        unique_key="case_id", 
        tags=["upskilling_completion","sl"]
    )
}}

with source_data as (
    select
        id,
        data
    from {{ source('staging_sl', 'upskilling_completion') }}
)

select
    id as case_id,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerator', '') as enumerator,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerators_first', '') as enumerators_first,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name', '') as "enumerator_last-name",
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'completed_upskilling_uf', '') as completed_upskilling_uf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'how_helpful_was_upskilling_uc', '') as how_helpful_was_upskilling_uc
from source_data


