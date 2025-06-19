{{ config(
    materialized='table',
    tags=['co_working', 'prod', 'visit_log']
) }}

SELECT
    case_id,
    date_of_visit,
    time_in,
    time_out,
    facilitator,
    enumerator_full_name,
    enumerator_first_name,
    enumerator_last_name,
    enumerator_username,
    training_or_service,
    purpose_for_using_csf,
    other_purpose_for_using_csf,
    using_coworking_to_earn_a_living,
    purpose_of_visit,
    shofco_training,
    'first_visit' AS visit_type
FROM {{ ref('staging_co_working_first_visit') }}

UNION ALL

SELECT
    case_id,
    date_of_visit,
    time_in,
    time_out,
    facilitator,
    enumerator_full_name,
    enumerator_first_name,
    enumerator_last_name,
    enumerator_username,
    specific_reason_for_accessing AS training_or_service,
    what_visitor_is_using AS purpose_for_using_csf,
    other_reason AS other_purpose_for_using_csf,
    using_coworking_for_living AS using_coworking_to_earn_a_living,
    purpose_of_visit,
    NULL AS shofco_training,
    'return_visit' AS visit_type
FROM {{ ref('staging_co_working_return_visit') }}