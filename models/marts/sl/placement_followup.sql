{{ config(
  materialized='table',
  tags=['placement_followup', 'sl']
) }}

SELECT
    id,
    case_id,
    user_id,
    enumerator,
    fullname,
    participant_id,
    age,
    sex,
    county,
    subcounty,
    ward,
    training_activity,
    placement_opportunity,
    type_of_business,
    participant_program,
    certifications_achieved,
    skills_gained,
    county_of_transition,
    region_of_transition,
    average_income_in_kes,
    date_received,

    -- Corrected column names
    new_role_source_pl AS new_role_source,
    documents_held_pl AS documents_held,
    sector_pl AS organization_sector,
    employment_type_pl AS nature_of_employment

FROM {{ ref('staging_sl_placement_followup') }}
