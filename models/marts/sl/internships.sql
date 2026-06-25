{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'Internship' as service,
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
    recommend_training_int as would_recommend,
    completed_training_int as completion_status,
    how_helpful_course_int as service_rating,
    completed_training_int,
    how_helpful_course_int,
    confidence_to_find_employment_int,
    recommend_training_int,
    clear_training_objective_int,
    content_relevant_to_needs_int,
    knowlegeable_and_engaging_int,
    effective_training_methods_int,
    feel_confident_int,
    inclusive_training_environment_int,
    satisfied_with_overall_training_int,
    {{ sl_likert_score('satisfied_with_overall_training_int') }} as satisfaction_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('feel_confident_int') }}),
            ({{ sl_likert_score('content_relevant_to_needs_int') }})
        ) as scores(score)
        where score is not null
    ) as comprehension_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('clear_training_objective_int') }}),
            ({{ sl_likert_score('knowlegeable_and_engaging_int') }}),
            ({{ sl_likert_score('effective_training_methods_int') }}),
            ({{ sl_likert_score('inclusive_training_environment_int') }})
        ) as scores(score)
        where score is not null
    ) as delivery_score,
    start_date_int,
    completion_date_int
from {{ ref('staging_sl_case_table') }}
where start_date_int is not null
   or completion_date_int is not null
   or completed_training_int is not null
   or how_helpful_course_int is not null
   or confidence_to_find_employment_int is not null
   or recommend_training_int is not null
   or clear_training_objective_int is not null
   or content_relevant_to_needs_int is not null
   or knowlegeable_and_engaging_int is not null
   or effective_training_methods_int is not null
   or feel_confident_int is not null
   or inclusive_training_environment_int is not null
   or satisfied_with_overall_training_int is not null
