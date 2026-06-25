{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

with placements as (
    select
        case_id,
        date_of_registration,
        pp_unique_id,
        pp_fullname,
        gender,
        nationality,
        refugee_type,
        kenyan_national_id_number_dir,
        county,
        subcounty,
        ward,
        primary_phone_number,
        phone_last_8_digits,
        is_pwd,
        type_of_disability_dir,
        is_young_mother,
        how_helpful_course_dl,
        how_helpful_course_tvet,
        how_helpful_course_ent,
        how_helpful_course_int,
        completed_training_dl,
        recommend_training_dl,
        recommend_training_tvet,
        recommend_training_ent,
        recommend_training_int,
        tvet_completion_status,
        completed_training_ent,
        completed_training_int,
        income_on_average_pl,
        placement_opportunity_pl
    from {{ ref('staging_sl_case_table') }}
    where (income_on_average_pl is not null and trim(income_on_average_pl) != '')
       or (placement_opportunity_pl is not null and trim(placement_opportunity_pl) != '')
),
participant_services as (
    select distinct
        case_id,
        service
    from {{ ref('cross_service_utilization') }}
)

select
    placements.case_id,
    placements.date_of_registration,
    placements.pp_unique_id,
    placements.pp_fullname,
    -- Job placements is intentionally exploded to one row per case_id per service undertaken
    -- so Superset can filter placement outcomes by any SL service.
    participant_services.service,
    placements.gender,
    placements.nationality,
    placements.refugee_type,
    placements.kenyan_national_id_number_dir,
    placements.county,
    placements.subcounty,
    placements.ward,
    placements.primary_phone_number,
    placements.phone_last_8_digits,
    placements.is_pwd,
    placements.type_of_disability_dir,
    placements.is_young_mother,
    case
        when participant_services.service = 'Digital Literacy' then placements.recommend_training_dl
        when participant_services.service = 'Internship' then placements.recommend_training_int
        when participant_services.service = 'TVET' then placements.recommend_training_tvet
        when participant_services.service = 'Entrepreneurship' then placements.recommend_training_ent
        else null
    end as would_recommend,
    case
        when participant_services.service = 'Digital Literacy' then placements.completed_training_dl
        when participant_services.service = 'Internship' then placements.completed_training_int
        when participant_services.service = 'TVET' then placements.tvet_completion_status
        when participant_services.service = 'Entrepreneurship' then placements.completed_training_ent
        else null
    end as completion_status,
    case
        when participant_services.service = 'Digital Literacy' then placements.how_helpful_course_dl
        when participant_services.service = 'Internship' then placements.how_helpful_course_int
        when participant_services.service = 'TVET' then placements.how_helpful_course_tvet
        when participant_services.service = 'Entrepreneurship' then placements.how_helpful_course_ent
        else null
    end as service_rating,
    placements.income_on_average_pl,
    placements.placement_opportunity_pl
from placements
join participant_services
    on placements.case_id = participant_services.case_id
