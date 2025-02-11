{% macro validate_date(field_name) %}
-- We have data coming from several google sheets and from commcare. below macro makes sure every data column, no matter
-- which table it is in or which source it is coming from is in the same appropriate format. 
-- Sometimes in google sheets, in one row someone enters 2024/10/12 and elsewhere someone enters 10-5-2024 it will standardize it

CASE
    -- Ensure NULL and empty values are handled safely
    WHEN {{ adapter.quote(field_name) }} IS NULL OR TRIM({{ adapter.quote(field_name) }}::text) = '' THEN NULL::DATE

    -- If already a DATE, return as is
    WHEN pg_typeof({{ adapter.quote(field_name) }})::text = 'date' THEN {{ adapter.quote(field_name) }}::date

    -- If TIMESTAMP, convert to DATE
    WHEN pg_typeof({{ adapter.quote(field_name) }})::text LIKE 'timestamp%' THEN {{ adapter.quote(field_name) }}::date

    -- 🔥 **Prioritize DD/MM/YYYY first**
    WHEN {{ adapter.quote(field_name) }}::text ~ '^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/\d{4}$' THEN 
        TO_DATE({{ adapter.quote(field_name) }}::text, 'DD/MM/YYYY')

    -- Handle YYYY-MM-DD format
    WHEN {{ adapter.quote(field_name) }}::text ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$' THEN 
        TO_DATE({{ adapter.quote(field_name) }}::text, 'YYYY-MM-DD')

    -- Handle DD-MM-YYYY format
    WHEN {{ adapter.quote(field_name) }}::text ~ '^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-\d{4}$' THEN 
        TO_DATE({{ adapter.quote(field_name) }}::text, 'DD-MM-YYYY')

    -- 🔥 **MM/DD/YYYY must match only valid months (01-12)**
    WHEN {{ adapter.quote(field_name) }}::text ~ '^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])/\d{4}$' THEN 
        TO_DATE({{ adapter.quote(field_name) }}::text, 'MM/DD/YYYY')

    -- Handle DD Mon YYYY format (e.g., 12 Jan 2024)
    WHEN {{ adapter.quote(field_name) }}::text ~ '^\d{1,2} \w{3} \d{4}$' THEN 
        TO_DATE({{ adapter.quote(field_name) }}::text, 'DD Mon YYYY')

    -- Default: Return NULL if no match
    ELSE NULL::DATE
END

{% endmacro %}

