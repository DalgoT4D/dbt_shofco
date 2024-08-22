{% macro validate_date(field_name) %}

-- Attempt to validate the field as a date, return the original format
case
    when {{ field_name }}::text ~ '^\s*\d{4}-\d{2}-\d{2}\s*$' then
        -- It strictly looks like a date (YYYY-MM-DD), safely cast to date
        {{ field_name }}::date
    else null
end

{% endmacro %}
