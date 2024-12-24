{{
    config(
        materialized="table",
        unique_key="case_id",
        alias="staging_facilities",
        tags= ["commcare_extraction", "wash_facilities"]
    )
}}

{% set commcare_case_type = "wash_facilities_case_data" %}
{% set case_type_properties_dict = {
    "ward": "ward",
    "county": "county",
    "village": "village",
    "latitude": "latitude",
    "owner_id": "owner_id",
    "longitude": "longitude",
    "subcounty": "subcounty",
    "date_opened": "date_facility_opened",
    "facility_id": "facility_id",
    "facility_name": "facility_name",
    "facility_type": "facility_type",
    "date_of_submission": "date_of_submission",
    "status_of_facility": "status_of_facility",
} -%}


with case_cte as ({{
    extract_case_table_from_wash_commcare_json(
        commcare_case_type, case_type_properties_dict, false
    )
}})

{{ dbt_utils.deduplicate(
    relation='case_cte',
    partition_by='case_id',
    order_by='indexed_on desc',
   )
}}
