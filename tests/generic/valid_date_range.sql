{% test valid_date_range(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE 
        {{ validate_date(column_name) }} IS NULL
         AND ( {{ column_name }} IS NOT NULL 
         AND TRIM({{ column_name }}::TEXT) <> '' )
{% endtest %}