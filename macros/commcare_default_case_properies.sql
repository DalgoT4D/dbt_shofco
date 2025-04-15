/*
this macro extracts default commcare case properties(these will be added to all the tables)
Parameters  
    has_parent: bool
        boolean value to indicate if the case type is a child case type. This is important to help map the parent child relationship
    
*/

{% docs commcare_default_case_properties %}
Extracts default CommCare case properties to be added to all applicable tables. If the case type has a parent (indicated by `has_parent`), additional fields for the parent-child relationship are included.

- `case_name`: The name of the case.
- `case_type`: The type of case.
- `parent_case_id`: The ID of the parent case (if applicable).
- `parent_case_type`: The type of the parent case (if applicable).
- `date_opened`: The date the case was opened.
- `case_id`: The unique identifier for the case.
- `indexed_on`: The timestamp when the case was indexed, in UTC.
- `closed`: Whether the case is closed.
- `created_at`: The timestamp when the record was created in the dbt model.
- 'date_modified': The date when the was was modified
{% enddocs %}



{% macro commcare_default_case_properies(has_parent=false) %}
    data -> 'properties' ->> 'case_name' as case_name,
	data -> 'properties' ->> 'case_type' as case_type,
     {% if has_parent %}
            data -> 'indices' -> 'parent' ->> 'case_id' as parent_case_id,
	        data -> 'indices' -> 'parent' ->> 'case_type' as parent_case_type,
    {% endif %}
	data -> 'properties' ->> 'date_opened' as date_opened,
	data ->> 'case_id' as case_id,
	((data ->> 'indexed_on')::timestamp without time zone at time zone 'Etc/UTC') as indexed_on,
	data ->> 'closed' as closed,
    data ->> 'date_modified' as date_modified,
    NOW() as created_at
{% endmacro %}