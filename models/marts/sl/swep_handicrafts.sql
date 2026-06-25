{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'Handcraft' as service,
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
    recommend_training_hc as would_recommend,
    completed_training_hc as completion_status,
    how_helpful_course_hc as service_rating,
    completed_training_hc,
    how_helpful_course_hc,
    confidence_to_find_employment_hc,
    recommend_training_hc,
    clear_training_objective_hc,
    content_relevant_to_needs_hc,
    knowlegeable_and_engaging_hc,
    effective_training_methods_hc,
    feel_confident_hc,
    inclusive_training_environment_hc,
    satisfied_with_overall_training_hc,
    {{ sl_likert_score('satisfied_with_overall_training_hc') }} as satisfaction_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('feel_confident_hc') }}),
            ({{ sl_likert_score('content_relevant_to_needs_hc') }})
        ) as scores(score)
        where score is not null
    ) as comprehension_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('clear_training_objective_hc') }}),
            ({{ sl_likert_score('knowlegeable_and_engaging_hc') }}),
            ({{ sl_likert_score('effective_training_methods_hc') }}),
            ({{ sl_likert_score('inclusive_training_environment_hc') }})
        ) as scores(score)
        where score is not null
    ) as delivery_score,
    swep_handcraft_center_hc,
    start_date_hc,
    completion_date_hc,
    handcraft_training_type_hc
from {{ ref('staging_sl_case_table') }}
where (swep_handcraft_center_hc is not null and trim(swep_handcraft_center_hc) != '')
   or start_date_hc is not null
   or completion_date_hc is not null
   or (handcraft_training_type_hc is not null and trim(handcraft_training_type_hc) != '')
   or completed_training_hc is not null
   or how_helpful_course_hc is not null
   or confidence_to_find_employment_hc is not null
   or recommend_training_hc is not null
   or clear_training_objective_hc is not null
   or content_relevant_to_needs_hc is not null
   or knowlegeable_and_engaging_hc is not null
   or effective_training_methods_hc is not null
   or feel_confident_hc is not null
   or inclusive_training_environment_hc is not null
   or satisfied_with_overall_training_hc is not null
