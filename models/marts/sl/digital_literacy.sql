{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'Digital Literacy' as service,
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
    recommend_training_dl as would_recommend,
    completed_training_dl as completion_status,
    how_helpful_course_dl as service_rating,
    completed_training_dl,
    final_exams_dl,
    type_of_exam_dl,
    how_helpful_course_dl,
    confidence_to_find_employment_dl,
    recommend_training_dl,
    clear_training_objective_dl,
    content_relevant_to_needs_dl,
    knowlegeable_and_engaging_dl,
    effective_training_methods_dl,
    feel_confident_dl,
    inclusive_training_environment_dl,
    satisfied_with_overall_training_dl,
    {{ sl_likert_score('satisfied_with_overall_training_dl') }} as satisfaction_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('feel_confident_dl') }}),
            ({{ sl_likert_score('content_relevant_to_needs_dl') }})
        ) as scores(score)
        where score is not null
    ) as comprehension_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('clear_training_objective_dl') }}),
            ({{ sl_likert_score('knowlegeable_and_engaging_dl') }}),
            ({{ sl_likert_score('effective_training_methods_dl') }}),
            ({{ sl_likert_score('inclusive_training_environment_dl') }})
        ) as scores(score)
        where score is not null
    ) as delivery_score,
    digital_literacy_dl,
    coalesce(start_date_dl, advanced_it_start_date_dl) as digital_literacy_start_date,
    coalesce(completion_date_dl, advanced_it_completion_date_dl) as digital_literacy_end_date,
    how_helpful_was_upskilling_uc,
    why_not_helpful_uc
from {{ ref('staging_sl_case_table') }}
where (digital_literacy_dl is not null and trim(digital_literacy_dl) != '')
   or completed_training_dl is not null
   or final_exams_dl is not null
   or type_of_exam_dl is not null
   or how_helpful_course_dl is not null
   or confidence_to_find_employment_dl is not null
   or recommend_training_dl is not null
   or clear_training_objective_dl is not null
   or content_relevant_to_needs_dl is not null
   or knowlegeable_and_engaging_dl is not null
   or effective_training_methods_dl is not null
   or feel_confident_dl is not null
   or inclusive_training_environment_dl is not null
   or satisfied_with_overall_training_dl is not null
