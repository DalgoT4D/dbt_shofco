{{
    config(
        materialized="table", 
        unique_key="case_id", 
        tags=["commcare_extraction","school_break_activities", "education"]
    )
}}


{% set commcare_case_type = "future_education_school_break_activities_case_data" %}
{% set case_type_properties_dict = {
    "owner_id": "owner_id",
    "name_of_training": "name_of_training",
} -%}


with school_break_activities_cte as ({{
    extract_case_table_from_education_commcare_json(
        commcare_case_type, case_type_properties_dict
    )
}})

{{ dbt_utils.deduplicate(
    relation='school_break_activities_cte',
    partition_by='case_id',
    order_by='indexed_on desc',
   )
}}
