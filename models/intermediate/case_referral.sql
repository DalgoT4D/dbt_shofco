{{ config(
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
    materialized='table'

) }}

with 
  case_cte as (
    select
      _airbyte_raw_id,
      "data" ->> 'indexed_on' AS indexed_on,
      id as id,
      "data" -> 'form' -> 'case' ->> '@case_id' AS case_id,
      "data" -> 'form' ->> 'case_name' as case_name,
      "data" -> 'form' -> 'subcase_0' -> 'case' -> 'create' ->> 'case_type' as case_type,
      "data" -> 'form' -> 'subcase_0' -> 'case' -> 'update' ->> 'survivor_gender' as gender,
      "data" -> 'form' -> 'subcase_0' -> 'case' -> 'update' ->> 'county_of_incident_report' as country,
      "data" -> 'form' -> 'subcase_0' -> 'case' -> 'update' ->> 'village_of_incident_report' as village
    from {{ source('source_commcare', 'case_occurence') }} )


{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='id',
    order_by='indexed_on desc',
   )
}}