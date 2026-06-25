{{ config(materialized='table', tags=['intermediate', 'sl', 'sl_mel']) }}

with normalized_programs as (
    select
        int_sl_mel_income.*,
        coalesce(
            nullif(
                regexp_replace(lower(coalesce(int_sl_mel_income.program_enrolled_raw, '')), '[^a-z0-9]+', ' ', 'g'),
                ''
            ),
            ''
        ) as program_enrolled_search_text
    from {{ ref('int_sl_mel_income') }} int_sl_mel_income
),

service_candidates as (
    select
        normalized_programs.*,
        array_remove(
            array[
                case when normalized_programs.program_enrolled_search_text ~ '(^| )wash( |$)' then 'WASH' end,
                case when normalized_programs.program_enrolled_search_text ~ '(^| )libraries?( |$)' then 'Libraries' end,
                case
                    when normalized_programs.program_enrolled_search_text ~ '(^| )health( |$)'
                        and normalized_programs.program_enrolled_search_text !~ 'mental health'
                        then 'Health'
                end,
                case
                    when normalized_programs.program_enrolled_search_text ~ '(^| )sun( |$)'
                        or normalized_programs.program_enrolled_search_text ~ 'sun youth forums'
                        then 'SUN'
                end,
                case
                    when normalized_programs.program_enrolled_search_text ~ '(^| )sacco( |$)'
                        or normalized_programs.program_enrolled_search_text ~ 'sacco loan'
                        then 'SACCO'
                end,
                case when normalized_programs.program_enrolled_search_text ~ '(^| )swep( |$)' then 'SWEP' end,
                case when normalized_programs.program_enrolled_search_text ~ 'business mentorship' then 'Business Mentorship' end,
                case when normalized_programs.program_enrolled_search_text ~ 'financial literacy' then 'Financial Literacy' end,
                case
                    when normalized_programs.program_enrolled_search_text ~ 'business grant'
                        or normalized_programs.program_enrolled_search_text ~ '(^| )grant(s)?( |$)'
                        then 'Business Grants'
                end,
                case when normalized_programs.program_enrolled_search_text ~ 'entrepreneur' then 'Entrepreneurship' end,
                case
                    when normalized_programs.program_enrolled_search_text ~ 'job readiness'
                        or normalized_programs.program_enrolled_search_text ~ 'employability'
                        then 'Job Readiness/Employability'
                end,
                case
                    when normalized_programs.program_enrolled_search_text ~ '(^| )tvet(s)?( |$)'
                        or normalized_programs.program_enrolled_search_text ~ 'vocational training'
                        then 'TVET'
                end,
                case when normalized_programs.program_enrolled_search_text ~ 'apprenticeship' then 'Apprenticeship' end,
                case when normalized_programs.program_enrolled_search_text ~ 'internship' then 'Internship' end,
                case when normalized_programs.program_enrolled_search_text ~ 'digital literacy' then 'Digital Literacy' end,
                case
                    when normalized_programs.program_enrolled_search_text ~ 'gender mental health'
                        or normalized_programs.program_enrolled_search_text ~ 'mental health'
                        or normalized_programs.program_enrolled_search_text ~ 'counselling'
                        or normalized_programs.program_enrolled_search_text ~ 'counseling'
                        or normalized_programs.program_enrolled_search_text ~ '(^| )gbv( |$)'
                        or normalized_programs.program_enrolled_search_text ~ 'gender related programs'
                        or normalized_programs.program_enrolled_search_text ~ '(^| )gender( |$)'
                        then 'Gender/Mental Health'
                end
            ],
            null
        ) as matched_services
    from normalized_programs
),

service_rows as (
    select
        service_candidates.evaluation_date,
        service_candidates.evaluation_program,
        service_candidates.respondent_name,
        service_candidates.gender,
        service_candidates.primary_telephone,
        service_candidates.alternative_telephone,
        service_candidates.date_of_birth,
        service_candidates.yob,
        service_candidates.age,
        service_candidates.nationality,
        service_candidates.national_id_number,
        service_candidates.por,
        service_candidates.education,
        service_candidates.county,
        service_candidates.subcounty,
        service_candidates.ward,
        service_candidates.with_disability,
        service_candidates.disability_type,
        service_candidates.other_disability_type,
        service_candidates.developmental_disability_disability_type,
        service_candidates.with_disability_certificate,
        service_candidates.is_sun_member,
        service_candidates.trained_with_shofco,
        service_candidates.date_enrolled,
        service_candidates.program_enrolled_raw,
        service_candidates.participation_type,
        service_candidates.swep_course_taken,
        service_candidates.specific_skills_acquired,
        service_candidates.training_duration,
        service_candidates.training_centre_institution_attended,
        service_candidates.completed_enrolled_program,
        service_candidates.exam_type,
        service_candidates.training_content_satisfaction,
        service_candidates.training_delivery_satisfaction,
        service_candidates.overall_training_satisfaction,
        service_candidates.feels_job_ready,
        service_candidates.currently_applying_training_skills,
        service_candidates.program_improvement_suggestions,
        service_candidates.had_skills_before_shofco_support,
        service_candidates.pre_shofco_skills,
        service_candidates.employment_status_before_shofco,
        service_candidates.employment_status_before_normalized,
        service_candidates.employed_before_flag,
        service_candidates.current_employment_status,
        service_candidates.current_employment_status_normalized,
        service_candidates.employed_after_flag,
        service_candidates.gained_employment_flag,
        service_candidates.lost_employment_flag,
        service_candidates.ran_business_before_shofco_support,
        service_candidates.business_type_before_shofco_support,
        service_candidates.business_branches_before_shofco_support,
        service_candidates.business_ownership_status_before_shofco_support,
        service_candidates.employees_before_shofco_support,
        service_candidates.business_registered_before_shofco_support,
        service_candidates.monthly_income_before_raw,
        service_candidates.monthly_income_before_band,
        service_candidates.monthly_income_before_lower_bound,
        service_candidates.monthly_income_before_upper_bound,
        service_candidates.monthly_income_before_estimated_amount,
        service_candidates.monthly_sales_before_shofco_support_raw,
        service_candidates.monthly_profits_before_shofco_support_raw,
        service_candidates.currently_running_business,
        service_candidates.current_business_type,
        service_candidates.current_business_branches,
        service_candidates.current_business_ownership_status,
        service_candidates.current_business_registered,
        service_candidates.created_employment_after_shofco_support,
        service_candidates.people_employed_after_shofco_support,
        service_candidates.employees_18_35_after_shofco_support,
        service_candidates.female_employees_18_35_after_shofco_support,
        service_candidates.male_employees_18_35_after_shofco_support,
        service_candidates.current_total_monthly_income_raw,
        service_candidates.current_business_total_monthly_income_raw,
        service_candidates.monthly_income_after_raw,
        service_candidates.monthly_income_after_band,
        service_candidates.monthly_income_after_lower_bound,
        service_candidates.monthly_income_after_upper_bound,
        service_candidates.monthly_income_after_estimated_amount,
        service_candidates.monthly_income_change_estimated_amount,
        service_candidates.income_increased_flag,
        service_candidates.current_total_monthly_sales_raw,
        service_candidates.current_total_monthly_profits_raw,
        service_candidates.job_satisfaction_current_work_environment_rating,
        service_candidates.job_satisfaction_current_work_life_balance_rating,
        service_candidates.job_satisfaction_income_enough_for_dependents,
        service_candidates.job_satisfaction_work_respected_by_others,
        service_candidates.job_satisfaction_feel_respected_at_work,
        service_candidates.job_satisfaction_work_gives_sense_of_purpose,
        services.service
    from service_candidates
    cross join lateral unnest(
        case
            when cardinality(service_candidates.matched_services) = 0
                and service_candidates.program_enrolled_raw is not null
                then array[service_candidates.program_enrolled_raw]
            else service_candidates.matched_services
        end
    ) as services(service)
)

select *
from service_rows
