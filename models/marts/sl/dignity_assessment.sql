{{ config(
    materialized='table',
    unique_key='case_id',
    tags=['sl', 'dignity_kit']
) }}

with upskilling as (
    select
        case_id,
        experienced_days_where_you_had_nothing as experienced_days_where_you_had_nothing_uf,
        miss_school_due_to_lack_hygiene_products as miss_school_due_to_lack_hygiene_products_uf,
        challenges_accessing_personal_hygiene_items as challenges_accessing_personal_hygiene_items_uf
    from {{ ref('upskilling') }}
),

service as (
    select
        case_id,
        dignity_experienced_days_nothing as experienced_days_where_you_had_nothing_sr,
        dignity_miss_school as miss_school_due_to_lack_hygiene_products_sr,
        dignity_challenges_hygiene as challenges_accessing_personal_hygiene_items_sr
    from {{ ref('service_enrollment') }}
)

select
    coalesce(u.case_id, s.case_id) as case_id,
    u.experienced_days_where_you_had_nothing_uf,
    u.miss_school_due_to_lack_hygiene_products_uf,
    u.challenges_accessing_personal_hygiene_items_uf,
    s.experienced_days_where_you_had_nothing_sr,
    s.miss_school_due_to_lack_hygiene_products_sr,
    s.challenges_accessing_personal_hygiene_items_sr
from upskilling u
full outer join service s
  on u.case_id = s.case_id