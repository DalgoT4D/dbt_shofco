{{ config(materialized='table', tags=['sl', 'sl_marts', 'sl_mel']) }}

with person_ranked as (
    select
        int_sl_mel_person_service_latest.*,
        row_number() over (
            partition by int_sl_mel_person_service_latest.person_key
            order by
                int_sl_mel_person_service_latest.evaluation_date desc nulls last,
                int_sl_mel_person_service_latest.date_enrolled desc nulls last,
                int_sl_mel_person_service_latest.current_employment_status_normalized desc nulls last
        ) as person_row_number
    from {{ ref('int_sl_mel_person_service_latest') }} int_sl_mel_person_service_latest
),

person_latest as (
    select *
    from person_ranked
    where person_row_number = 1
),

service_rollup as (
    select
        int_sl_mel_person_service_latest.person_key,
        count(*) as service_count,
        case
            when count(*) = 1 then 'Single service'
            else 'Multi-service'
        end as pathway_type,
        string_agg(int_sl_mel_person_service_latest.service, ' + ' order by int_sl_mel_person_service_latest.service) as service_combo,
        max(case
            when int_sl_mel_person_service_latest.employees_before_shofco_support between 0 and 100000
                then int_sl_mel_person_service_latest.employees_before_shofco_support
        end) as jobs_before_shofco,
        max(case
            when int_sl_mel_person_service_latest.people_employed_after_shofco_support between 0 and 100000
                then int_sl_mel_person_service_latest.people_employed_after_shofco_support
        end) as jobs_after_shofco,
        max(case when int_sl_mel_person_service_latest.service = 'Apprenticeship' then 1 else 0 end) as has_apprenticeship,
        max(case when int_sl_mel_person_service_latest.service = 'Business Grants' then 1 else 0 end) as has_business_grants,
        max(case when int_sl_mel_person_service_latest.service = 'Business Mentorship' then 1 else 0 end) as has_business_mentorship,
        max(case when int_sl_mel_person_service_latest.service = 'Digital Literacy' then 1 else 0 end) as has_digital_literacy,
        max(case when int_sl_mel_person_service_latest.service = 'Entrepreneurship' then 1 else 0 end) as has_entrepreneurship,
        max(case when int_sl_mel_person_service_latest.service = 'Financial Literacy' then 1 else 0 end) as has_financial_literacy,
        max(case when int_sl_mel_person_service_latest.service = 'Gender/Mental Health' then 1 else 0 end) as has_gender_mental_health,
        max(case when int_sl_mel_person_service_latest.service = 'Internship' then 1 else 0 end) as has_internship,
        max(case when int_sl_mel_person_service_latest.service = 'Job Readiness/Employability' then 1 else 0 end) as has_job_readiness_employability,
        max(case when int_sl_mel_person_service_latest.service = 'SACCO' then 1 else 0 end) as has_sacco,
        max(case when int_sl_mel_person_service_latest.service = 'SUN' then 1 else 0 end) as has_sun,
        max(case when int_sl_mel_person_service_latest.service = 'SWEP' then 1 else 0 end) as has_swep,
        max(case when int_sl_mel_person_service_latest.service = 'TVET' then 1 else 0 end) as has_tvet
    from {{ ref('int_sl_mel_person_service_latest') }} int_sl_mel_person_service_latest
    group by 1
)

select
    person_latest.person_key,
    {{ dbt_utils.star(from=ref('int_sl_mel_service_participation'), relation_alias='person_latest', except=['service', 'with_disability']) }},
    case
        when service_rollup.service_count = 1 then service_rollup.service_combo
        else 'Multiple services'
    end as service,
    person_latest.service as latest_service_recorded,
    person_latest.with_disability as is_pwd,
    service_rollup.pathway_type,
    service_rollup.service_count,
    service_rollup.service_combo,
    service_rollup.jobs_before_shofco,
    service_rollup.jobs_after_shofco,
    coalesce(service_rollup.jobs_after_shofco, 0) - coalesce(service_rollup.jobs_before_shofco, 0) as net_jobs_change_after_shofco,
    greatest(coalesce(service_rollup.jobs_after_shofco, 0) - coalesce(service_rollup.jobs_before_shofco, 0), 0) as jobs_created_because_of_shofco,
    case
        when coalesce(service_rollup.jobs_after_shofco, 0) - coalesce(service_rollup.jobs_before_shofco, 0) > 0 then 1
        else 0
    end as youth_creating_jobs_because_of_shofco_flag,
    case when service_rollup.service_count = 1 then 1 else 0 end as single_service_only_flag,
    case when service_rollup.service_count > 1 then 1 else 0 end as multi_service_flag,
    case when person_latest.employed_before_flag = 0 then 1 else 0 end as unemployed_before_flag,
    case
        when person_latest.employed_before_flag = 0 and person_latest.employed_after_flag = 1 then 1
        else 0
    end as gained_employment_from_unemployment_flag,
    service_rollup.has_apprenticeship,
    service_rollup.has_business_grants,
    service_rollup.has_business_mentorship,
    service_rollup.has_digital_literacy,
    service_rollup.has_entrepreneurship,
    service_rollup.has_financial_literacy,
    service_rollup.has_gender_mental_health,
    service_rollup.has_internship,
    service_rollup.has_job_readiness_employability,
    service_rollup.has_sacco,
    service_rollup.has_sun,
    service_rollup.has_swep,
    service_rollup.has_tvet
from person_latest
inner join service_rollup
    on person_latest.person_key = service_rollup.person_key
