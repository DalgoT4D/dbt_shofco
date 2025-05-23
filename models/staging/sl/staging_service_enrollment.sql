{{
    config(
        materialized="table", 
        unique_key="case_id", 
        tags=["commcare_extraction","service_enrollment","sl"]
    )
}}

with source_data as (
    select
        id,
        data
    from {{ source('staging_sl', 'service_enroll') }}
)

select
    id as case_id,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'pp_fullname', '') as pp_fullname,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'pp_sex', '') as pp_sex,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'pp_age', '') as pp_age,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'pp_unique_id', '') as pp_unique_id,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'course_enrolled_sr', '') as course_enrolled_sr,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'ongoing_course', '') as ongoing_course,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'region', '') as region,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_county', '') as pp_shofco_county,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_subcounty', '') as pp_shofco_subcounty,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'pp_ahofco_ward', '') as pp_ahofco_ward,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'participants_program_sr', '') as participants_program_sr,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'user_location_id', '') as user_location_id,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'count_casedb', '') as count_casedb,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'date_first_visit_bm', '') as date_first_visit_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'type_of_business_bm', '') as type_of_business_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'mentee_business_records_bm', '') as mentee_business_records_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'if_mentee_have_dead_stock_bm', '') as if_mentee_have_dead_stock_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'cash_to_buy_new_stock_bm', '') as cash_to_buy_new_stock_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'business_costs_before_setting_prices_bm', '') as business_costs_before_setting_prices_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'mentees_price_compared_to_competitors_bm', '') as mentees_price_compared_to_competitors_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'stock_bm', '') as stock_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'employees_bm', '') as employees_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'visit_bm', '') as visit_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'county_bm', '') as county_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'bus_location_bm', '') as bus_location_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'name_of_mentor_bm', '') as name_of_mentor_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'contact_of_mentor_bm', '') as contact_of_mentor_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'challenges_mentee_is_facing_bm', '') as challenges_mentee_is_facing_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'solutions_agreed_to_be_implemented_bm', '') as solutions_agreed_to_be_implemented_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'agreed_timeline_to_implement_solutions_bm', '') as agreed_timeline_to_implement_solutions_bm,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerators_first', '') as enumerators_first,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name', '') as enumerator_lastname,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerator', '') as enumerator
from source_data


