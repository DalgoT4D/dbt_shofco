{% macro normalize_gender(field_name) %}
{%- set cleaned_gender = "regexp_replace(lower(trim(" ~ field_name ~ "::text)), '[^a-z]+', '', 'g')" -%}
case
    when {{ field_name }} is null or trim({{ field_name }}::text) = '' then null
    when {{ cleaned_gender }} in ('f', 'female', 'femfale')
        or {{ cleaned_gender }} like 'female%' then 'female'
    when {{ cleaned_gender }} in ('m', 'male')
        or {{ cleaned_gender }} like 'male%' then 'male'
    else 'other'
end
{% endmacro %}
