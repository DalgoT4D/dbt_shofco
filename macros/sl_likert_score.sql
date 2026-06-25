{% macro sl_likert_score(field_name) %}
{%- set normalized_value = "regexp_replace(lower(trim(" ~ field_name ~ "::text)), '[^a-z0-9]+', '', 'g')" -%}
case
    when {{ field_name }} is null or trim({{ field_name }}::text) = '' then null::numeric
    when trim({{ field_name }}::text) ~ '^[1-5](\\.0+)?$' then trim({{ field_name }}::text)::numeric
    when {{ normalized_value }} in ('1', 'stronglydisagree', 'verydissatisfied', 'veryunsatisfied') then 1::numeric
    when {{ normalized_value }} in ('2', 'disagree', 'dissatisfied', 'unsatisfied') then 2::numeric
    when {{ normalized_value }} in ('3', 'neutral', 'neitheragreenordisagree', 'average', 'notsure') then 3::numeric
    when {{ normalized_value }} in ('4', 'agree', 'satisfied') then 4::numeric
    when {{ normalized_value }} in ('5', 'stronglyagree', 'verysatisfied') then 5::numeric
    else null::numeric
end
{% endmacro %}
