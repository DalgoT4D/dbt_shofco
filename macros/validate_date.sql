{% macro validate_date(field_name) %}
-- We have data coming from several google sheets and from commcare. below macro makes sure every data column, no matter
-- which table it is in or which source it is coming from is in the same appropriate format. 
-- Sometimes in google sheets, in one row someone enters 2024/10/12 and elsewhere someone enters 10-5-2024 it will standardize it
case
    -- Ensure NULL and empty strings are converted to NULL safely before anything else
    when {{ field_name }} is null or COALESCE({{ field_name }}::text, '') = '' then null::date

    -- If it's already a DATE, return it as is
    when pg_typeof({{ field_name }})::text = 'date' then 
        {{ field_name }}::date

    -- Ensures that TIMESTAMP and TIMESTAMP WITH TIME ZONE are converted properly
    when pg_typeof({{ field_name }})::text like 'timestamp%' then 
        {{ field_name }}::date

    -- Handle standard YYYY-MM-DD format
    when COALESCE({{ field_name }}::text, '') ~ '^\d{4}-\d{2}-\d{2}$' then 
        to_date({{ field_name }}::text, 'YYYY-MM-DD')

    -- Handle DD-MM-YYYY format
    when COALESCE({{ field_name }}::text, '') ~ '^\d{2}-\d{2}-\d{4}$' then 
        to_date({{ field_name }}::text, 'DD-MM-YYYY')

    -- Handle MM/DD/YYYY format
    when COALESCE({{ field_name }}::text, '') ~ '^\d{2}/\d{2}/\d{4}$' then 
        to_date({{ field_name }}::text, 'MM/DD/YYYY')

    -- Handle DD Mon YYYY format (e.g., 12 Jan 2024)
    when COALESCE({{ field_name }}::text, '') ~ '^\d{1,2} \w{3} \d{4}$' then 
        to_date({{ field_name }}::text, 'DD Mon YYYY')

    -- If none of the formats match, return NULL
    else null::date
end
{% endmacro %}