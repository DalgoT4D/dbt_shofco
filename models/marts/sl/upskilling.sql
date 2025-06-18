{{
  config(
    materialized='table',
    tags=['upskilling', 'sl', 'beneficiary_tracking']
  )
}}

WITH registration AS (
  SELECT
    id AS registration_id,
    case_id,
    user_id,
    fullname,
    first_name,
    last_name,
    training_program,
    swep_center,
    year_trained,
    additional_support,
    educational_certificates,
    has_children,
    children_under_4,
    children_in_daycare,
    children_with_disabilities,
    willing_to_relocate,
    willing_to_be_upskilled,
    upskilling_component,
    recommendation_status,
    date_received
  FROM {{ ref('staging_upskilling_form') }}
),

completion AS (
  SELECT
    case_id AS completion_case_id,
    enumerator AS completion_enumerator,
    enumerators_first AS completion_enumerators_first,
    "enumerator_last-name" AS completion_enumerator_last_name,
    completed_upskilling_uf,
    how_helpful_was_upskilling_uc
  FROM {{ ref('staging_upskilling_completion') }}
)

SELECT
  r.registration_id,
  r.case_id,
  r.user_id,
  r.fullname,
  r.first_name,
  r.last_name,
  r.training_program,
  r.swep_center,
  r.year_trained,
  r.additional_support,
  r.educational_certificates,
  r.has_children,
  r.children_under_4,
  r.children_in_daycare,
  r.children_with_disabilities,
  r.willing_to_relocate,
  r.willing_to_be_upskilled,
  r.upskilling_component,
  r.recommendation_status,
  r.date_received,

  c.completion_enumerator,
  c.completion_enumerators_first,
  c.completion_enumerator_last_name,
  c.completed_upskilling_uf,
  c.how_helpful_was_upskilling_uc

FROM registration r
LEFT JOIN completion c
  ON r.case_id = c.completion_case_id