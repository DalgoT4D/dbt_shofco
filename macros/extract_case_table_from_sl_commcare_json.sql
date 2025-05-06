/*
this macro extracts specific columns from case json data gotten from commcare api
Parameters  
    commcare_case_type: str
        commcare case_type name
    case_type_properties_dict: dict
        key, value python style dictionary with the db column name(key) and alias(value)
    has_parent: bool
        boolean value to indicate if the case type is a child case type
    
*/

{% docs extract_case_table_from_commcare_json %}
Extracts and transforms case JSON data from the CommCare API into a structured format suitable for analysis. It selects case type-specific properties defined in the `case_type_properties_dict` parameter and includes default case properties using the `commcare_default_case_properties` macro. It filters the data to include only the specified `commcare_case_type` and non-closed cases.

Parameters:
- `commcare_case_type`: The name of the CommCare case type to filter the data.
- `case_type_properties_dict`: A dictionary mapping the database column name (key) to the JSON alias (value) for case type-specific properties.
- `has_parent`: A boolean flag to indicate if the case type is a child case type, affecting the inclusion of parent case fields.

The macro is designed to work with incremental models in dbt, including an additional filter during incremental runs to only select records that are new or updated since the last run.
{% enddocs %}


{% macro extract_case_table_from_sl_commcare_json(commcare_case_type, case_type_properties_dict, has_parent=false) %}
    
    WITH commcare_data as (
    SELECT 
        -- case type specific properties
        {% for case_type_property, alias in case_type_properties_dict.items() %}
            data -> 'properties' ->> '{{case_type_property}}' as {{alias}},
        {% endfor %}
        -- default properties added by commcare
        {{commcare_default_case_properies(has_parent)}}
    FROM  {{ source('staging_sl','raw_case') }}
    WHERE data -> 'properties' ->> 'case_type' = '{{commcare_case_type}}'
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    -- (uses >= to include records arriving later on the same day as the last run of this model)
    AND ((data ->> 'indexed_on')::timestamp without time zone at time zone 'Etc/UTC') >= (select max(indexed_on) from {{ this }})
    {% endif %}

    )

SELECT DISTINCT * FROM commcare_data
{% endmacro %}