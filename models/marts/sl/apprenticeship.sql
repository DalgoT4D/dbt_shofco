{{ config(materialized='table', tags=['sl', 'sl_marts']) }}

select 
    case_id,
    date_of_registration,
    pp_unique_id,
    pp_fullname,
    'Apprenticeship' as service,
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
    recommend_training_apr as would_recommend,
    completed_training_apr as completion_status,
    how_helpful_course_apr as service_rating,
    completed_training_apr,
    how_helpful_course_apr,
    confidence_to_find_employment_apr,
    recommend_training_apr,
    clear_training_objective_apr,
    content_relevant_to_needs_apr,
    knowlegeable_and_engaging_apr,
    effective_training_methods_apr,
    feel_confident_apr,
    inclusive_training_environment_apr,
    satisfied_with_overall_training_apr,
    {{ sl_likert_score('satisfied_with_overall_training_apr') }} as satisfaction_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('feel_confident_apr') }}),
            ({{ sl_likert_score('content_relevant_to_needs_apr') }})
        ) as scores(score)
        where score is not null
    ) as comprehension_score,
    (
        select avg(score)::numeric(10,2)
        from (values
            ({{ sl_likert_score('clear_training_objective_apr') }}),
            ({{ sl_likert_score('knowlegeable_and_engaging_apr') }}),
            ({{ sl_likert_score('effective_training_methods_apr') }}),
            ({{ sl_likert_score('inclusive_training_environment_apr') }})
        ) as scores(score)
        where score is not null
    ) as delivery_score,
    apprenticeship_provider_apr,
    skill_enrolled_apr,
    skill_name,
    sector_name,
    placement_date_apr,
    gps_of_business_location_apbl_raw,
    apprenticeship_latitude,
    apprenticeship_longitude,
    apprenticeship_altitude,
    apprenticeship_accuracy
from {{ ref('staging_sl_case_table') }}
where (apprenticeship_provider_apr is not null and trim(apprenticeship_provider_apr) != '')
   or gps_of_business_location_apbl_raw is not null
   or completed_training_apr is not null
   or how_helpful_course_apr is not null
   or confidence_to_find_employment_apr is not null
   or recommend_training_apr is not null
   or clear_training_objective_apr is not null
   or content_relevant_to_needs_apr is not null
   or knowlegeable_and_engaging_apr is not null
   or effective_training_methods_apr is not null
   or feel_confident_apr is not null
   or inclusive_training_environment_apr is not null
   or satisfied_with_overall_training_apr is not null
