{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'TVET' as service,
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
    recommend_training_tvet as would_recommend,
    coalesce(completed_training_tvet, tvet_completion_status) as completion_status,
    how_helpful_course_tvet as service_rating,
    completed_training_tvet,
    internal_exam_tvet,
    confidence_to_find_employment_tvet,
    clear_training_objective_tvet,
    content_relevant_to_needs_tvet,
    knowlegeable_and_engaging_tvet,
    effective_training_methods_tvet,
    feel_confident_tvet,
    inclusive_training_environment_tvet,
    satisfied_with_overall_training_tvet,
    {{ sl_likert_score('satisfied_with_overall_training_tvet') }} as satisfaction_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('feel_confident_tvet') }}),
            ({{ sl_likert_score('content_relevant_to_needs_tvet') }})
        ) as scores(score)
        where score is not null
    ) as comprehension_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('clear_training_objective_tvet') }}),
            ({{ sl_likert_score('knowlegeable_and_engaging_tvet') }}),
            ({{ sl_likert_score('effective_training_methods_tvet') }}),
            ({{ sl_likert_score('inclusive_training_environment_tvet') }})
        ) as scores(score)
        where score is not null
    ) as delivery_score,
    name_of_institution_tvet,
    name_of_facilitator,
    course_enrolled_tvet,
    start_date_tvet,
    completion_date_tvet,
    nita_exams,
    how_helpful_course_tvet,
    recommend_training_tvet,
    location_of_institution_ttia_raw,
    tvet_latitude,
    tvet_longitude,
    tvet_altitude,
    tvet_accuracy
from {{ ref('staging_sl_case_table') }}
where (name_of_institution_tvet is not null and trim(name_of_institution_tvet) != '')
   or (course_enrolled_tvet is not null and trim(course_enrolled_tvet) != '')
   or start_date_tvet is not null
   or location_of_institution_ttia_raw is not null
   or completed_training_tvet is not null
   or internal_exam_tvet is not null
   or how_helpful_course_tvet is not null
   or confidence_to_find_employment_tvet is not null
   or recommend_training_tvet is not null
   or clear_training_objective_tvet is not null
   or content_relevant_to_needs_tvet is not null
   or knowlegeable_and_engaging_tvet is not null
   or effective_training_methods_tvet is not null
   or feel_confident_tvet is not null
   or inclusive_training_environment_tvet is not null
   or satisfied_with_overall_training_tvet is not null
