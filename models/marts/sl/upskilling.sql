{{
  config(
    materialized='table',
    tags=['upskilling', 'sl', 'daycare', 'dignity_kit', "sl_marts"]
  )
}}

SELECT
  id AS registration_id,
  case_id,
  user_id,
  pp_fullname AS fullname,
  training_program_uf AS training_program,
  swep_center_uf AS swep_center,
  year_trained_on_uf AS year_trained,
  additional_support_uf AS additional_support,
  educational_certificates_uf AS educational_certificates,
  
  
  -- Other fields
  willing_to_relocate_uf AS willing_to_relocate,
  willing_to_be_upskilled_uf AS willing_to_be_upskilled,
  component_of_service_to_be_upskilled AS upskilling_component,
  do_you_recommend_beneficiary_to_proceed_uf AS recommendation_status,
  indexed_on AS date_received,

  -- Completion fields
  enumerator AS completion_enumerator,
  enumerator_first_Name AS completion_enumerator_first_name,  -- Using coalesced field
  enumerator_last_name AS completion_enumerator_last_name,  -- Using coalesced field
  completed_upskilling_uf AS completed_upskilling,
  how_helpful_was_upskilling_uc AS how_helpful_was_upskilling

FROM {{ ref('staging_sl_case_table') }}
WHERE willing_to_be_upskilled_uf IS NOT NULL
   OR completed_upskilling_uf IS NOT NULL