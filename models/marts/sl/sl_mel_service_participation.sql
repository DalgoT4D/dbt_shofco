{{ config(materialized='table', tags=['sl', 'sl_marts', 'sl_mel']) }}

select
    {{ dbt_utils.star(from=ref('int_sl_mel_person_service_latest'), relation_alias='int_sl_mel_person_service_latest', except=['with_disability']) }},
    int_sl_mel_person_service_latest.with_disability as is_pwd
from {{ ref('int_sl_mel_person_service_latest') }} int_sl_mel_person_service_latest
