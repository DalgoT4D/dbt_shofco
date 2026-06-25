{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'Entrepreneurship' as service,
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
    recommend_training_ent as would_recommend,
    completed_training_ent as completion_status,
    how_helpful_course_ent as service_rating,
    completed_training_ent,
    how_helpful_course_ent,
    confidence_to_find_employment_ent,
    recommend_training_ent,
    clear_training_objective_ep,
    content_relevant_to_needs_ep,
    knowlegeable_and_engaging_ep,
    effective_training_methods_ep,
    feel_confident_ep,
    inclusive_training_environment_ep,
    satisfied_with_overall_training_ep,
    {{ sl_likert_score('satisfied_with_overall_training_ep') }} as satisfaction_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('feel_confident_ep') }}),
            ({{ sl_likert_score('content_relevant_to_needs_ep') }})
        ) as scores(score)
        where score is not null
    ) as comprehension_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('clear_training_objective_ep') }}),
            ({{ sl_likert_score('knowlegeable_and_engaging_ep') }}),
            ({{ sl_likert_score('effective_training_methods_ep') }}),
            ({{ sl_likert_score('inclusive_training_environment_ep') }})
        ) as scores(score)
        where score is not null
    ) as delivery_score,
    start_date_ent,
    completion_date_ent,
    interest_in_sales_work_ent
from {{ ref('staging_sl_case_table') }}
where start_date_ent is not null
   or completion_date_ent is not null
   or interest_in_sales_work_ent is not null
   or completed_training_ent is not null
   or how_helpful_course_ent is not null
   or confidence_to_find_employment_ent is not null
   or recommend_training_ent is not null
   or clear_training_objective_ep is not null
   or content_relevant_to_needs_ep is not null
   or knowlegeable_and_engaging_ep is not null
   or effective_training_methods_ep is not null
   or feel_confident_ep is not null
   or inclusive_training_environment_ep is not null
   or satisfied_with_overall_training_ep is not null
