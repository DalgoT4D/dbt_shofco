{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'Tailoring' as service,
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
    recommend_training_st as would_recommend,
    completed_training_st as completion_status,
    how_helpful_course_st as service_rating,
    completed_training_st,
    reason_not_completed_st,
    internal_exam_st,
    how_helpful_course_st,
    confidence_to_find_employment_st,
    recommend_training_st,
    clear_training_objective_st,
    content_relevant_to_needs_st,
    knowlegeable_and_engaging_st,
    effective_training_methods_st,
    feel_confident_st,
    inclusive_training_environment_st,
    satisfied_with_overall_training_st,
    {{ sl_likert_score('satisfied_with_overall_training_st') }} as satisfaction_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('feel_confident_st') }}),
            ({{ sl_likert_score('content_relevant_to_needs_st') }})
        ) as scores(score)
        where score is not null
    ) as comprehension_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('clear_training_objective_st') }}),
            ({{ sl_likert_score('knowlegeable_and_engaging_st') }}),
            ({{ sl_likert_score('effective_training_methods_st') }}),
            ({{ sl_likert_score('inclusive_training_environment_st') }})
        ) as scores(score)
        where score is not null
    ) as delivery_score,
    swep_tailoring_center_st,
    start_date_of_training_st,
    expected_end_date_of_training_st,
    training_session_st,
    nita_exams
from {{ ref('staging_sl_case_table') }}
where (swep_tailoring_center_st is not null and trim(swep_tailoring_center_st) != '')
   or start_date_of_training_st is not null
   or expected_end_date_of_training_st is not null
   or (training_session_st is not null and trim(training_session_st) != '')
   or completed_training_st is not null
   or reason_not_completed_st is not null
   or internal_exam_st is not null
   or how_helpful_course_st is not null
   or confidence_to_find_employment_st is not null
   or recommend_training_st is not null
   or clear_training_objective_st is not null
   or content_relevant_to_needs_st is not null
   or knowlegeable_and_engaging_st is not null
   or effective_training_methods_st is not null
   or feel_confident_st is not null
   or inclusive_training_environment_st is not null
   or satisfied_with_overall_training_st is not null
