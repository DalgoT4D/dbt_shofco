{{ config(materialized='table', tags=['intermediate', 'sl', 'sl_mel']) }}

with source_data as (
    select *
    from {{ ref('staging_sl_mel_database') }}
),

cleaned as (
    select
        evaluation_date,
        {{ clean_sheet_text('evaluation_program') }} as evaluation_program,
        {{ clean_sheet_text('respondent_name') }} as respondent_name,
        {{ normalize_gender(clean_sheet_text('gender')) }} as gender,
        {{ clean_sheet_phone('primary_telephone') }} as primary_telephone,
        {{ clean_sheet_phone('alternative_telephone') }} as alternative_telephone,
        date_of_birth,
        {{ clean_sheet_integer('yob') }} as yob,
        {{ clean_sheet_integer('age') }} as age,
        {{ clean_sheet_text('nationality') }} as nationality,
        nullif(
            regexp_replace(
                coalesce({{ clean_sheet_text('national_id_number') }}::text, ''),
                '[^0-9A-Za-z]+',
                '',
                'g'
            ),
            ''
        ) as national_id_number,
        {{ clean_sheet_text('por') }} as por,
        {{ clean_sheet_text('education') }} as education,
        {{ clean_sheet_geography('county') }} as county,
        {{ clean_sheet_geography('subcounty') }} as subcounty,
        {{ clean_sheet_geography('ward') }} as ward,
        {{ normalize_yes_no('with_disability') }} as with_disability,
        {{ clean_sheet_text('disability_type') }} as disability_type,
        {{ clean_sheet_text('other_disability_type') }} as other_disability_type,
        {{ clean_sheet_text('developmental_disability_disability_type') }} as developmental_disability_disability_type,
        {{ normalize_yes_no('with_disability_certificate') }} as with_disability_certificate,
        {{ normalize_yes_no('is_sun_member') }} as is_sun_member,
        {{ normalize_yes_no('trained_with_shofco') }} as trained_with_shofco,
        date_enrolled,
        case
            when lower(coalesce({{ clean_sheet_text('program_enrolled_raw') }}::text, '')) = 'none' then null
            else {{ clean_sheet_text('program_enrolled_raw') }}
        end as program_enrolled_raw,
        {{ clean_sheet_text('participation_type') }} as participation_type,
        {{ clean_sheet_text('swep_course_taken') }} as swep_course_taken,
        {{ clean_sheet_text('specific_skills_acquired') }} as specific_skills_acquired,
        {{ clean_sheet_text('training_duration') }} as training_duration,
        {{ clean_sheet_text('training_centre_institution_attended') }} as training_centre_institution_attended,
        {{ normalize_yes_no('completed_enrolled_program') }} as completed_enrolled_program,
        {{ clean_sheet_text('exam_type') }} as exam_type,
        {{ clean_sheet_text('training_content_satisfaction') }} as training_content_satisfaction,
        {{ clean_sheet_text('training_delivery_satisfaction') }} as training_delivery_satisfaction,
        {{ clean_sheet_text('overall_training_satisfaction') }} as overall_training_satisfaction,
        {{ normalize_yes_no('feels_job_ready') }} as feels_job_ready,
        {{ normalize_yes_no('currently_applying_training_skills') }} as currently_applying_training_skills,
        {{ clean_sheet_text('program_improvement_suggestions') }} as program_improvement_suggestions,
        {{ normalize_yes_no('had_skills_before_shofco_support') }} as had_skills_before_shofco_support,
        {{ clean_sheet_text('pre_shofco_skills') }} as pre_shofco_skills,
        {{ clean_sheet_text('employment_status_before_shofco') }} as employment_status_before_shofco,
        {{ normalize_yes_no('ran_business_before_shofco_support') }} as ran_business_before_shofco_support,
        {{ clean_sheet_text('business_type_before_shofco_support') }} as business_type_before_shofco_support,
        {{ clean_sheet_integer('business_branches_before_shofco_support') }} as business_branches_before_shofco_support,
        {{ clean_sheet_text('business_ownership_status_before_shofco_support') }} as business_ownership_status_before_shofco_support,
        {{ clean_sheet_integer('employees_before_shofco_support') }} as employees_before_shofco_support,
        {{ normalize_yes_no('business_registered_before_shofco_support') }} as business_registered_before_shofco_support,
        {{ clean_sheet_text('total_monthly_income_before_shoco_support_raw') }} as monthly_income_before_raw,
        {{ clean_sheet_text('monthly_sales_before_shofco_support_raw') }} as monthly_sales_before_shofco_support_raw,
        {{ clean_sheet_text('monthly_profits_before_shofco_support_raw') }} as monthly_profits_before_shofco_support_raw,
        {{ clean_sheet_text('current_employment_status') }} as current_employment_status,
        {{ normalize_yes_no('currently_running_business') }} as currently_running_business,
        {{ clean_sheet_text('current_business_type') }} as current_business_type,
        {{ clean_sheet_integer('current_business_branches') }} as current_business_branches,
        {{ clean_sheet_text('current_business_ownership_status') }} as current_business_ownership_status,
        {{ normalize_yes_no('current_business_registered') }} as current_business_registered,
        {{ normalize_yes_no('created_employment_after_shofco_support') }} as created_employment_after_shofco_support,
        {{ clean_sheet_integer('people_employed_after_shofco_support') }} as people_employed_after_shofco_support,
        {{ clean_sheet_integer('employees_18_35_after_shofco_support') }} as employees_18_35_after_shofco_support,
        {{ clean_sheet_integer('female_employees_18_35_after_shofco_support') }} as female_employees_18_35_after_shofco_support,
        {{ clean_sheet_integer('male_employees_18_35_after_shofco_support') }} as male_employees_18_35_after_shofco_support,
        {{ clean_sheet_text('current_total_monthly_income_raw') }} as current_total_monthly_income_raw,
        {{ clean_sheet_text('current_business_total_monthly_income_raw') }} as current_business_total_monthly_income_raw,
        coalesce(
            {{ clean_sheet_text('current_total_monthly_income_raw') }},
            {{ clean_sheet_text('current_business_total_monthly_income_raw') }}
        ) as monthly_income_after_raw,
        {{ clean_sheet_text('current_total_monthly_sales_raw') }} as current_total_monthly_sales_raw,
        {{ clean_sheet_text('current_total_monthly_profits_raw') }} as current_total_monthly_profits_raw,
        {{ clean_sheet_text('job_satisfaction_current_work_environment_rating') }} as job_satisfaction_current_work_environment_rating,
        {{ clean_sheet_text('job_satisfaction_current_work_life_balance_rating') }} as job_satisfaction_current_work_life_balance_rating,
        {{ clean_sheet_text('job_satisfaction_income_enough_for_dependents') }} as job_satisfaction_income_enough_for_dependents,
        {{ clean_sheet_text('job_satisfaction_work_respected_by_others') }} as job_satisfaction_work_respected_by_others,
        {{ clean_sheet_text('job_satisfaction_feel_respected_at_work') }} as job_satisfaction_feel_respected_at_work,
        {{ clean_sheet_text('job_satisfaction_work_gives_sense_of_purpose') }} as job_satisfaction_work_gives_sense_of_purpose
    from source_data
),

employment_normalized as (
    select
        cleaned.*,
        case
            when cleaned.employment_status_before_shofco is null then null
            when lower(cleaned.employment_status_before_shofco) like '%unemployed%' then 'Unemployed/Not in any IGA'
            when lower(cleaned.employment_status_before_shofco) like '%self-employed%'
                or lower(cleaned.employment_status_before_shofco) like '%self employed%'
                or lower(cleaned.employment_status_before_shofco) like '%running a business%'
                then 'Self-employed/Running a Business'
            when lower(cleaned.employment_status_before_shofco) like '%casual or temporary worker%'
                or lower(cleaned.employment_status_before_shofco) like '%part-time%'
                or lower(cleaned.employment_status_before_shofco) like '%part time%'
                then 'Casual or Temporary Worker/Part-time'
            when lower(cleaned.employment_status_before_shofco) like '%full-time%'
                or lower(cleaned.employment_status_before_shofco) like '%full time%'
                then 'Employed Full-time'
            when lower(cleaned.employment_status_before_shofco) like '%contract%'
                or lower(cleaned.employment_status_before_shofco) like '%mkataba%'
                then 'Employed on a Contract'
            when lower(cleaned.employment_status_before_shofco) like '%volunteer%' then 'Volunteer'
            when lower(cleaned.employment_status_before_shofco) like '%other%' then 'Other'
            else cleaned.employment_status_before_shofco
        end as employment_status_before_normalized,
        case
            when cleaned.current_employment_status is null then null
            when lower(cleaned.current_employment_status) like '%unemployed%' then 'Unemployed/Not in any IGA'
            when lower(cleaned.current_employment_status) like '%self-employed%'
                or lower(cleaned.current_employment_status) like '%self employed%'
                or lower(cleaned.current_employment_status) like '%running a business%'
                then 'Self-employed/Running a Business'
            when lower(cleaned.current_employment_status) like '%casual or temporary worker%'
                or lower(cleaned.current_employment_status) like '%part-time%'
                or lower(cleaned.current_employment_status) like '%part time%'
                then 'Casual or Temporary Worker/Part-time'
            when lower(cleaned.current_employment_status) like '%full-time%'
                or lower(cleaned.current_employment_status) like '%full time%'
                then 'Employed Full-time'
            when lower(cleaned.current_employment_status) like '%contract%'
                or lower(cleaned.current_employment_status) like '%mkataba%'
                then 'Employed on a Contract'
            when lower(cleaned.current_employment_status) like '%volunteer%' then 'Volunteer'
            when lower(cleaned.current_employment_status) like '%other%' then 'Other'
            else cleaned.current_employment_status
        end as current_employment_status_normalized
    from cleaned
),

employment_enriched as (
    select
        employment_normalized.*,
        case
            when employment_status_before_normalized is null then null
            when employment_status_before_normalized in (
                'Self-employed/Running a Business',
                'Casual or Temporary Worker/Part-time',
                'Employed Full-time',
                'Employed on a Contract'
            ) then 1
            when employment_status_before_normalized in (
                'Unemployed/Not in any IGA',
                'Volunteer',
                'Other'
            ) then 0
            else null
        end as employed_before_flag,
        case
            when current_employment_status_normalized is null then null
            when current_employment_status_normalized in (
                'Self-employed/Running a Business',
                'Casual or Temporary Worker/Part-time',
                'Employed Full-time',
                'Employed on a Contract'
            ) then 1
            when current_employment_status_normalized in (
                'Unemployed/Not in any IGA',
                'Volunteer',
                'Other'
            ) then 0
            else null
        end as employed_after_flag
    from employment_normalized
),

income_cleaned as (
    select
        employment_enriched.*,
        case
            when employment_enriched.monthly_income_before_raw is null then null
            when lower(trim(employment_enriched.monthly_income_before_raw)) in ('none', 'nil') then '0'
            else trim(employment_enriched.monthly_income_before_raw)
        end as monthly_income_before_clean,
        case
            when employment_enriched.monthly_income_after_raw is null then null
            when lower(trim(employment_enriched.monthly_income_after_raw)) in ('none', 'nil') then '0'
            else trim(employment_enriched.monthly_income_after_raw)
        end as monthly_income_after_clean
    from employment_enriched
),

income_prepared as (
    select
        income_cleaned.*,
        regexp_replace(
            replace(replace(lower(income_cleaned.monthly_income_before_clean), '–', '-'), '—', '-'),
            '\s+',
            ' ',
            'g'
        ) as monthly_income_before_normalized_text,
        regexp_replace(
            regexp_replace(
                replace(replace(lower(income_cleaned.monthly_income_before_clean), '–', '-'), '—', '-'),
                '\s+',
                ' ',
                'g'
            ),
            '[^0-9-]+',
            '',
            'g'
        ) as monthly_income_before_normalized_digits,
        regexp_replace(
            replace(replace(lower(income_cleaned.monthly_income_after_clean), '–', '-'), '—', '-'),
            '\s+',
            ' ',
            'g'
        ) as monthly_income_after_normalized_text,
        regexp_replace(
            regexp_replace(
                replace(replace(lower(income_cleaned.monthly_income_after_clean), '–', '-'), '—', '-'),
                '\s+',
                ' ',
                'g'
            ),
            '[^0-9-]+',
            '',
            'g'
        ) as monthly_income_after_normalized_digits
    from income_cleaned
),

income_bounds as (
    select
        income_prepared.*,
        case
            when monthly_income_before_clean is null then null
            when monthly_income_before_normalized_text ~ '(over|above)'
                and monthly_income_before_normalized_digits ~ '^[0-9]+$'
                then monthly_income_before_normalized_digits::numeric + 1
            when monthly_income_before_normalized_digits ~ '^[0-9]+-[0-9]+$'
                then least(
                    split_part(monthly_income_before_normalized_digits, '-', 1)::numeric,
                    split_part(monthly_income_before_normalized_digits, '-', 2)::numeric
                )
            when monthly_income_before_normalized_digits ~ '^[0-9]+$'
                then monthly_income_before_normalized_digits::numeric
            else null
        end as monthly_income_before_lower_bound,
        case
            when monthly_income_before_clean is null then null
            when monthly_income_before_normalized_text ~ '(over|above)'
                and monthly_income_before_normalized_digits ~ '^[0-9]+$'
                then null
            when monthly_income_before_normalized_digits ~ '^[0-9]+-[0-9]+$'
                then greatest(
                    split_part(monthly_income_before_normalized_digits, '-', 1)::numeric,
                    split_part(monthly_income_before_normalized_digits, '-', 2)::numeric
                )
            when monthly_income_before_normalized_digits ~ '^[0-9]+$'
                then monthly_income_before_normalized_digits::numeric
            else null
        end as monthly_income_before_upper_bound,
        case
            when monthly_income_after_clean is null then null
            when monthly_income_after_normalized_text ~ '(over|above)'
                and monthly_income_after_normalized_digits ~ '^[0-9]+$'
                then monthly_income_after_normalized_digits::numeric + 1
            when monthly_income_after_normalized_digits ~ '^[0-9]+-[0-9]+$'
                then least(
                    split_part(monthly_income_after_normalized_digits, '-', 1)::numeric,
                    split_part(monthly_income_after_normalized_digits, '-', 2)::numeric
                )
            when monthly_income_after_normalized_digits ~ '^[0-9]+$'
                then monthly_income_after_normalized_digits::numeric
            else null
        end as monthly_income_after_lower_bound,
        case
            when monthly_income_after_clean is null then null
            when monthly_income_after_normalized_text ~ '(over|above)'
                and monthly_income_after_normalized_digits ~ '^[0-9]+$'
                then null
            when monthly_income_after_normalized_digits ~ '^[0-9]+-[0-9]+$'
                then greatest(
                    split_part(monthly_income_after_normalized_digits, '-', 1)::numeric,
                    split_part(monthly_income_after_normalized_digits, '-', 2)::numeric
                )
            when monthly_income_after_normalized_digits ~ '^[0-9]+$'
                then monthly_income_after_normalized_digits::numeric
            else null
        end as monthly_income_after_upper_bound
    from income_prepared
)

select
    evaluation_date,
    evaluation_program,
    respondent_name,
    gender,
    primary_telephone,
    alternative_telephone,
    date_of_birth,
    yob,
    age,
    {{ normalize_sl_nationality_filter('nationality', 'null', 'por') }} as nationality,
    national_id_number,
    por,
    education,
    {{ normalize_sl_county_filter('county') }} as county,
    {{ normalize_sl_subcounty_filter('subcounty') }} as subcounty,
    ward,
    {{ normalize_sl_yes_no_filter('with_disability') }} as with_disability,
    disability_type,
    other_disability_type,
    developmental_disability_disability_type,
    with_disability_certificate,
    is_sun_member,
    trained_with_shofco,
    date_enrolled,
    program_enrolled_raw,
    participation_type,
    swep_course_taken,
    specific_skills_acquired,
    training_duration,
    training_centre_institution_attended,
    completed_enrolled_program,
    exam_type,
    training_content_satisfaction,
    training_delivery_satisfaction,
    overall_training_satisfaction,
    feels_job_ready,
    currently_applying_training_skills,
    program_improvement_suggestions,
    had_skills_before_shofco_support,
    pre_shofco_skills,
    employment_status_before_shofco,
    employment_status_before_normalized,
    employed_before_flag,
    current_employment_status,
    current_employment_status_normalized,
    employed_after_flag,
    case
        when employed_before_flag = 0 and employed_after_flag = 1 then 1
        when employed_before_flag is null or employed_after_flag is null then null
        else 0
    end as gained_employment_flag,
    case
        when employed_before_flag = 1 and employed_after_flag = 0 then 1
        when employed_before_flag is null or employed_after_flag is null then null
        else 0
    end as lost_employment_flag,
    ran_business_before_shofco_support,
    business_type_before_shofco_support,
    business_branches_before_shofco_support,
    business_ownership_status_before_shofco_support,
    employees_before_shofco_support,
    business_registered_before_shofco_support,
    monthly_income_before_raw,
    case
        when monthly_income_before_lower_bound is null then null
        when monthly_income_before_upper_bound is null then to_char(monthly_income_before_lower_bound, 'FM999G999G999') || '+'
        when monthly_income_before_lower_bound = monthly_income_before_upper_bound then to_char(monthly_income_before_lower_bound, 'FM999G999G999')
        else
            to_char(monthly_income_before_lower_bound, 'FM999G999G999')
            || '-'
            || to_char(monthly_income_before_upper_bound, 'FM999G999G999')
    end as monthly_income_before_band,
    monthly_income_before_lower_bound,
    monthly_income_before_upper_bound,
    case
        when monthly_income_before_lower_bound is null then null
        when monthly_income_before_upper_bound is null then monthly_income_before_lower_bound
        else (monthly_income_before_lower_bound + monthly_income_before_upper_bound) / 2.0
    end as monthly_income_before_estimated_amount,
    monthly_sales_before_shofco_support_raw,
    monthly_profits_before_shofco_support_raw,
    currently_running_business,
    current_business_type,
    current_business_branches,
    current_business_ownership_status,
    current_business_registered,
    created_employment_after_shofco_support,
    people_employed_after_shofco_support,
    employees_18_35_after_shofco_support,
    female_employees_18_35_after_shofco_support,
    male_employees_18_35_after_shofco_support,
    current_total_monthly_income_raw,
    current_business_total_monthly_income_raw,
    monthly_income_after_raw,
    case
        when monthly_income_after_lower_bound is null then null
        when monthly_income_after_upper_bound is null then to_char(monthly_income_after_lower_bound, 'FM999G999G999') || '+'
        when monthly_income_after_lower_bound = monthly_income_after_upper_bound then to_char(monthly_income_after_lower_bound, 'FM999G999G999')
        else
            to_char(monthly_income_after_lower_bound, 'FM999G999G999')
            || '-'
            || to_char(monthly_income_after_upper_bound, 'FM999G999G999')
    end as monthly_income_after_band,
    monthly_income_after_lower_bound,
    monthly_income_after_upper_bound,
    case
        when monthly_income_after_lower_bound is null then null
        when monthly_income_after_upper_bound is null then monthly_income_after_lower_bound
        else (monthly_income_after_lower_bound + monthly_income_after_upper_bound) / 2.0
    end as monthly_income_after_estimated_amount,
    case
        when monthly_income_before_lower_bound is null or monthly_income_after_lower_bound is null then null
        when monthly_income_before_upper_bound is null and monthly_income_after_upper_bound is null
            then monthly_income_after_lower_bound - monthly_income_before_lower_bound
        when monthly_income_before_upper_bound is null
            then ((monthly_income_after_lower_bound + monthly_income_after_upper_bound) / 2.0) - monthly_income_before_lower_bound
        when monthly_income_after_upper_bound is null
            then monthly_income_after_lower_bound - ((monthly_income_before_lower_bound + monthly_income_before_upper_bound) / 2.0)
        else
            ((monthly_income_after_lower_bound + monthly_income_after_upper_bound) / 2.0)
            - ((monthly_income_before_lower_bound + monthly_income_before_upper_bound) / 2.0)
    end as monthly_income_change_estimated_amount,
    case
        when monthly_income_before_lower_bound is null or monthly_income_after_lower_bound is null then null
        when (
            case
                when monthly_income_before_upper_bound is null and monthly_income_after_upper_bound is null
                    then monthly_income_after_lower_bound - monthly_income_before_lower_bound
                when monthly_income_before_upper_bound is null
                    then ((monthly_income_after_lower_bound + monthly_income_after_upper_bound) / 2.0) - monthly_income_before_lower_bound
                when monthly_income_after_upper_bound is null
                    then monthly_income_after_lower_bound - ((monthly_income_before_lower_bound + monthly_income_before_upper_bound) / 2.0)
                else
                    ((monthly_income_after_lower_bound + monthly_income_after_upper_bound) / 2.0)
                    - ((monthly_income_before_lower_bound + monthly_income_before_upper_bound) / 2.0)
            end
        ) > 0 then 1
        else 0
    end as income_increased_flag,
    current_total_monthly_sales_raw,
    current_total_monthly_profits_raw,
    job_satisfaction_current_work_environment_rating,
    job_satisfaction_current_work_life_balance_rating,
    job_satisfaction_income_enough_for_dependents,
    job_satisfaction_work_respected_by_others,
    job_satisfaction_feel_respected_at_work,
    job_satisfaction_work_gives_sense_of_purpose
from income_bounds
