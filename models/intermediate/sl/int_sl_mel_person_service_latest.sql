{{ config(materialized='table', tags=['intermediate', 'sl', 'sl_mel']) }}

with base as (
    select
        int_sl_mel_service_participation.*,
        nullif(
            regexp_replace(lower(coalesce(int_sl_mel_service_participation.respondent_name, '')), '[^a-z0-9]+', '', 'g'),
            ''
        ) as name_key,
        lower(regexp_replace(coalesce(int_sl_mel_service_participation.national_id_number, ''), '[^0-9a-z]+', '', 'g')) as national_id_key,
        right(regexp_replace(coalesce(int_sl_mel_service_participation.primary_telephone, ''), '[^0-9]+', '', 'g'), 9) as primary_phone_key,
        right(regexp_replace(coalesce(int_sl_mel_service_participation.alternative_telephone, ''), '[^0-9]+', '', 'g'), 9) as alternative_phone_key
    from {{ ref('int_sl_mel_service_participation') }} int_sl_mel_service_participation
),

cleaned as (
    select
        base.*,
        case
            when base.national_id_key = ''
                or base.national_id_key in (
                    '0', '00', '000', '0000', '000000',
                    '999', '9999', '99999', '999999', '9999999', '99999999', '999999999', '9999999999',
                    'waiting', 'waitingcard', 'refused', 'notgiven', 'nil', 'none', 'na', 'n/a', 'w', 'wc'
                )
                or base.national_id_key like 'waiting%'
                or base.national_id_key ~ '^(.)\1{4,}$'
                then null
            else base.national_id_key
        end as valid_national_id_key,
        case
            when base.primary_phone_key = ''
                or base.primary_phone_key ~ '^(.)\1{8}$'
                then null
            else base.primary_phone_key
        end as valid_primary_phone_key,
        case
            when base.alternative_phone_key = ''
                or base.alternative_phone_key ~ '^(.)\1{8}$'
                then null
            else base.alternative_phone_key
        end as valid_alternative_phone_key
    from base
),

identified as (
    select
        cleaned.*,
        coalesce(
            case
                when cleaned.valid_national_id_key is not null
                    then 'nid:' || cleaned.valid_national_id_key
            end,
            case
                when cleaned.valid_primary_phone_key is not null and cleaned.name_key is not null
                    then 'phone_name:' || cleaned.valid_primary_phone_key || '|' || cleaned.name_key
            end,
            case
                when cleaned.valid_alternative_phone_key is not null and cleaned.name_key is not null
                    then 'alt_phone_name:' || cleaned.valid_alternative_phone_key || '|' || cleaned.name_key
            end,
            case
                when cleaned.name_key is not null and cleaned.date_of_birth is not null
                    then 'name_dob:' || cleaned.name_key || '|' || cleaned.date_of_birth::text
            end,
            case
                when cleaned.name_key is not null and cleaned.yob is not null
                    then 'name_yob:' || cleaned.name_key || '|' || cleaned.yob::text
            end,
            case
                when cleaned.name_key is not null and cleaned.county is not null
                    then 'name_county:' || cleaned.name_key || '|' || lower(cleaned.county)
            end,
            'name_only:' || coalesce(cleaned.name_key, 'missing')
        ) as person_key
    from cleaned
),

ranked as (
    select
        identified.*,
        row_number() over (
            partition by identified.person_key, identified.service
            order by
                identified.evaluation_date desc nulls last,
                identified.date_enrolled desc nulls last,
                identified.current_employment_status_normalized desc nulls last
        ) as service_row_number
    from identified
)

select
    ranked.person_key,
    {{ dbt_utils.star(from=ref('int_sl_mel_service_participation'), relation_alias='ranked') }}
from ranked
where ranked.service_row_number = 1
