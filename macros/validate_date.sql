{% macro validate_date(field_name) %}
-- We have data coming from several google sheets and from commcare. below macro makes sure every data column, no matter
-- which table it is in or which source it is coming from is in the same appropriate format. 
-- Sometimes in google sheets, in one row someone enters 2024/10/05 and elsewhere someone enters 10-5-2024 it will standardize it
-- If the extracted year is before 1950 or after 2050, it is assumed to be incorrect and returns NULL.

CASE
    -- Ensure NULL and empty values are handled safely
    WHEN {{ field_name }} IS NULL OR TRIM({{ field_name }}::TEXT) = '' THEN NULL::DATE

    -- If already a DATE, return as is (but check valid range)
    WHEN pg_typeof({{ field_name }})::TEXT = 'date' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM {{ field_name }}::DATE) BETWEEN 1950 AND 2050 
            THEN {{ field_name }}::DATE
            ELSE NULL 
        END
        
     -- Handle strict DD/MM/YYYY format (e.g., 01/09/2024 or 9/9/2024)
    WHEN {{ field_name }}::TEXT ~ '^([0-2]?[0-9]|3[01])/([0]?[1-9]|1[0-2])/[1-2]\d{3}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'DD/MM/YYYY')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'DD/MM/YYYY') 
            ELSE NULL 
        END

     -- Handle ISO 8601 format (e.g., 2024-02-05T08:52:24.859000Z)
    WHEN {{ field_name }}::TEXT ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$' THEN
        CASE 
            WHEN EXTRACT(YEAR FROM TO_TIMESTAMP({{ field_name }}::TEXT, 'YYYY-MM-DD"T"HH24:MI:SS.US')) BETWEEN 1950 AND 2050 
            THEN TO_TIMESTAMP({{ field_name }}::TEXT, 'YYYY-MM-DD"T"HH24:MI:SS.US')::DATE
            ELSE NULL 
        END

    -- If TIMESTAMP, convert to DATE (but check valid range)
    WHEN pg_typeof({{ field_name }})::TEXT LIKE 'timestamp%' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM {{ field_name }}::DATE) BETWEEN 1950 AND 2050 
            THEN {{ field_name }}::DATE
            ELSE NULL 
        END
    
        -- Handle DD/M/YY (e.g., 15/5/24 → 2024-05-15)
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\d{2}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM (TO_DATE({{ field_name }}::TEXT, 'DD/M/YY') + INTERVAL '2000 years')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'DD/M/YY') + INTERVAL '2000 years' 
            ELSE NULL 
        END
    
        -- Handle DD-M-YY (e.g., 15-5-24 → 2024-05-15)
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\d{2}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM (TO_DATE({{ field_name }}::TEXT, 'DD-M-YY') + INTERVAL '2000 years')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'DD-M-YY') + INTERVAL '2000 years' 
            ELSE NULL 
        END

    -- Handle DD-M-YYYY (e.g., 15-5-2024 → 2024-05-15)
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\d{4}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'DD-M-YYYY')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'DD-M-YYYY') 
            ELSE NULL 
        END

    -- Handle DD/M/YYYY (e.g., 15/5/2024 → 2024-05-15)
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\d{4}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'DD/M/YYYY')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'DD/M/YYYY') 
            ELSE NULL 
        END

    -- Handle YYYY-MM-DD 
    WHEN {{ field_name }}::TEXT ~ '^\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'YYYY-MM-DD')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'YYYY-MM-DD') 
            ELSE NULL 
        END

    -- Handle YYYY/MM/DD 
    WHEN {{ field_name }}::TEXT ~ '^\d{4}/(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'YYYY/MM/DD')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'YYYY/MM/DD') 
            ELSE NULL 
        END

    -- Handle MM-DD-YYYY 
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])-\d{4}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'MM-DD-YYYY')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'MM-DD-YYYY') 
            ELSE NULL 
        END

    -- Handle MM/DD/YYYY 
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\d{4}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'MM/DD/YYYY')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'MM/DD/YYYY') 
            ELSE NULL 
        END

    -- Handle DD Mon YYYY (e.g., 12 Jan 2024)
    WHEN {{ field_name }}::TEXT ~ '^\d{1,2} \w{3} \d{4}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'DD Mon YYYY')) BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'DD Mon YYYY') 
            ELSE NULL 
        END

    -- Handle MM/DD/YY (e.g., 08/05/24 → 2024-08-05)
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\d{2}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'MM/DD/YY') + INTERVAL '2000 years') BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'MM/DD/YY') + INTERVAL '2000 years' 
            ELSE NULL 
        END

    -- Handle MM-DD-YY (e.g., 08-05-24 → 2024-08-05)
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])-\d{2}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'MM-DD-YY') + INTERVAL '2000 years') BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'MM-DD-YY') + INTERVAL '2000 years' 
            ELSE NULL 
        END

    -- Handle DD/MM/YY (e.g., 05/08/24 → 2024-08-05)
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\d{2}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'DD/MM/YY') + INTERVAL '2000 years') BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'DD/MM/YY') + INTERVAL '2000 years' 
            ELSE NULL 
        END

    -- Handle DD-MM-YY (e.g., 05-08-24 → 2024-08-05)
    WHEN {{ field_name }}::TEXT ~ '^(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\d{2}$' THEN 
        CASE 
            WHEN EXTRACT(YEAR FROM TO_DATE({{ field_name }}::TEXT, 'DD-MM-YY') + INTERVAL '2000 years') BETWEEN 1950 AND 2050 
            THEN TO_DATE({{ field_name }}::TEXT, 'DD-MM-YY') + INTERVAL '2000 years' 
            ELSE NULL 
        END

    -- If no match, return NULL
    ELSE NULL::DATE
END
{% endmacro %}