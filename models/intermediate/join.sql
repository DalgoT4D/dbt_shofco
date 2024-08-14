{{ config(
   materialized='table'
) }}

with 
  -- Select data from the 'case' model
  case_data as (
    select 
      id as case_id,
      case_name,
      case_type,
      gender,
      country,
      village,
      closed,
      date_closed
    from {{ ref('case') }}
  ),

  -- Select data from the 'case_occurrence' model
  case_occurrence_data as (
    select
      id as occurrence_id,
      case_id,
      case_name as occurrence_case_name,
      case_type as occurrence_case_type,
      gender as occurrence_gender,
      country as occurrence_country,
      village as occurrence_village
    from {{ ref('case_occurence') }}
  )

-- Perform an inner join on case_id
select
  COALESCE(case_data.case_id, case_occurrence_data.case_id) as case_id,
  case_data.case_name,
  case_data.case_type,
  case_data.gender,
  case_data.country,
  case_data.village,
  case_data.closed,
  case_data.date_closed,
  case_occurrence_data.occurrence_id,
  case_occurrence_data.occurrence_case_name,
  case_occurrence_data.occurrence_case_type,
  case_occurrence_data.occurrence_gender,
  case_occurrence_data.occurrence_country,
  case_occurrence_data.occurrence_village
from case_data
full outer join case_occurrence_data
on case_data.case_id = case_occurrence_data.case_id