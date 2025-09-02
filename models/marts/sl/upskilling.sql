{{
  config(
    materialized='table',
    tags=['upskilling', 'sl', 'daycare', 'dignity_kit']
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
    
    -- Dignity Kit support fields
    head_gender,
    receiving_aid,
    household_head,
    dignity_kit_support,
    hh_engaged_in_work,
    forego_basic_essentials,
    number_of_meals_reduced,
    experienced_days_where_you_had_nothing,
    miss_school_due_to_lack_hygiene_products,
    challenges_accessing_personal_hygiene_items,
    
    -- Daycare support fields
    has_children,
    children_under_4,
    number_of_children,
    daycare_support_details,
    children_in_daycare,
    children_with_disabilities,
    
    -- Other fields
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
    enumerator_first_name AS completion_enumerator_first_name,
    enumerator_last_name AS completion_enumerator_last_name,
    completed_upskilling,
    how_helpful_was_upskilling
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
  
  -- Dignity Kit support fields
  r.head_gender,
  r.receiving_aid,
  r.household_head,
  r.dignity_kit_support,
  r.hh_engaged_in_work,
  r.forego_basic_essentials,
  r.number_of_meals_reduced,
  r.experienced_days_where_you_had_nothing,
  r.miss_school_due_to_lack_hygiene_products,
  r.challenges_accessing_personal_hygiene_items,
  
  -- Daycare support fields
  r.has_children,
  r.children_under_4,
  r.number_of_children,
  r.daycare_support_details,
  r.children_in_daycare,
  r.children_with_disabilities,
  
  -- Other fields
  r.willing_to_relocate,
  r.willing_to_be_upskilled,
  r.upskilling_component,
  r.recommendation_status,
  r.date_received,

  -- Completion fields
  c.completion_enumerator,
  c.completion_enumerator_first_name,
  c.completion_enumerator_last_name,
  c.completed_upskilling,
  c.how_helpful_was_upskilling

FROM registration r
JOIN completion c
  ON r.case_id = c.completion_case_id