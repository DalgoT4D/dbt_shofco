{{ config(
    materialized='table',
    unique_key='case_id',
    tags=['sl', 'daycare']
) }}

with upskilling as (
    select
        case_id,
        children_under_4,
        children_with_disabilities,
        has_children as number_of_children_uf
    from {{ ref('upskilling') }}
),

service as (
    select
        case_id,
        daycare_children_under_4 as children_under_4_sr,
        daycare_children_with_disabilities as children_with_disabilities_sr,
        daycare_number_of_children as number_of_children_sr
    from {{ ref('service_enrollment') }}
)

select
    coalesce(u.case_id, s.case_id) as case_id,
    u.number_of_children_uf,
    u.children_under_4,
    u.children_with_disabilities,
    s.number_of_children_sr,
    s.children_under_4_sr,
    s.children_with_disabilities_sr
from upskilling u
full outer join service s
  on u.case_id = s.case_id