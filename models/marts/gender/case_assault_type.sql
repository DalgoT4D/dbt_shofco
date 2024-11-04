{{ config(
  materialized='table'
) }}

with base_data as (
    select
        case_id,
        case_county_name as county,
        age_group,
        survivor_gender,
        date_of_case_reporting,
        assault_type
    from {{ ref('case_occurence') }}
)

select
    case_id,
    county,
    age_group,
    survivor_gender,
    date_of_case_reporting,
    case
        when assault_type ilike '%juvenile%' then 'Juvenile Case'
        when assault_type ilike '%domestic%' then 'Domestic Violence'
        when assault_type ilike '%sexual%' then 'Sexual Violence'
        when assault_type ilike '%physical%' then 'Physical Violence'
        when assault_type ilike '%negligence%' then 'Negligence'
        when assault_type ilike '%legal%' or assault_type ilike '%counsel%' then 'Seeking Legal Counsel'
        else 'Other'
    end as cleaned_assault_type
from base_data