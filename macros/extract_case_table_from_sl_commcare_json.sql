{% macro extract_case_table_from_sl_commcare_json(commcare_case_type, case_type_properties_dict, has_parent=false) %}

    WITH commcare_data as (
    SELECT 
        -- case type specific properties
        {% for case_type_property, alias in case_type_properties_dict.items() %}
            data -> 'properties' ->> '{{case_type_property}}' as {{alias}},
        {% endfor %}

        -- default commcare case fields
        {{ commcare_default_case_properies(has_parent) }}

    FROM {{ source('staging_sl', 'zzz_case') }}
    WHERE data -> 'properties' ->> 'case_type' = '{{ commcare_case_type }}'
    {% if is_incremental() %}
        AND ((data ->> 'indexed_on')::timestamp without time zone at time zone 'Etc/UTC') >= (SELECT MAX(indexed_on) FROM {{ this }})
    {% endif %}
    )

SELECT DISTINCT * FROM commcare_data

{% endmacro %}
