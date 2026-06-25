{% macro clean_sheet_text(value_expr) -%}
case
    when {{ value_expr }} is null then null
    when lower(trim({{ value_expr }}::text)) in ('', '---', '--', 'null', 'n/a', 'na') then null
    else trim({{ value_expr }}::text)
end
{%- endmacro %}

{% macro clean_sheet_phone(value_expr) -%}
nullif(
    regexp_replace(
        coalesce({{ clean_sheet_text(value_expr) }}::text, ''),
        '[^0-9]+',
        '',
        'g'
    ),
    ''
)
{%- endmacro %}

{% macro clean_sheet_integer(value_expr) -%}
case
    when {{ clean_sheet_text(value_expr) }} is null then null
    when regexp_replace({{ clean_sheet_text(value_expr) }}::text, ',', '', 'g') ~ '^-?[0-9]+$'
        then regexp_replace({{ clean_sheet_text(value_expr) }}::text, ',', '', 'g')::integer
    when regexp_replace({{ clean_sheet_text(value_expr) }}::text, ',', '', 'g') ~ '^-?[0-9]+\\.0+$'
        then split_part(regexp_replace({{ clean_sheet_text(value_expr) }}::text, ',', '', 'g'), '.', 1)::integer
    else null
end
{%- endmacro %}

{% macro clean_sheet_numeric(value_expr) -%}
case
    when {{ clean_sheet_text(value_expr) }} is null then null
    when regexp_replace({{ clean_sheet_text(value_expr) }}::text, ',', '', 'g') ~ '^-?[0-9]+(\\.[0-9]+)?$'
        then regexp_replace({{ clean_sheet_text(value_expr) }}::text, ',', '', 'g')::numeric
    else null
end
{%- endmacro %}

{% macro normalize_yes_no(value_expr) -%}
case
    when {{ clean_sheet_text(value_expr) }} is null then null
    when regexp_replace(lower({{ clean_sheet_text(value_expr) }}::text), '[^a-z0-9]+', '', 'g') in ('yes', 'y', 'true', '1')
        then 'yes'
    when regexp_replace(lower({{ clean_sheet_text(value_expr) }}::text), '[^a-z0-9]+', '', 'g') in ('no', 'n', 'false', '0')
        then 'no'
    else lower(trim({{ clean_sheet_text(value_expr) }}::text))
end
{%- endmacro %}

{% macro clean_sheet_date(value_expr) -%}
{%- set cleaned_value = clean_sheet_text(value_expr) -%}
case
    when {{ cleaned_value }} is null then null
    when {{ cleaned_value }}::text ~ '^[0-9]{5}(\\.[0-9]+)?$'
        then date '1899-12-30' + split_part({{ cleaned_value }}::text, '.', 1)::integer

    when {{ cleaned_value }}::text ~ '^\\d{4}-\\d{2}-\\d{2}$'
        and split_part({{ cleaned_value }}::text, '-', 2)::integer between 1 and 12
        and split_part({{ cleaned_value }}::text, '-', 3)::integer between 1 and extract(
            day from (
                date_trunc(
                    'month',
                    make_date(
                        split_part({{ cleaned_value }}::text, '-', 1)::integer,
                        split_part({{ cleaned_value }}::text, '-', 2)::integer,
                        1
                    ) + interval '1 month'
                ) - interval '1 day'
            )::date
        )
        then make_date(
            split_part({{ cleaned_value }}::text, '-', 1)::integer,
            split_part({{ cleaned_value }}::text, '-', 2)::integer,
            split_part({{ cleaned_value }}::text, '-', 3)::integer
        )

    when {{ cleaned_value }}::text ~ '^\\d{4}/\\d{2}/\\d{2}$'
        and split_part({{ cleaned_value }}::text, '/', 2)::integer between 1 and 12
        and split_part({{ cleaned_value }}::text, '/', 3)::integer between 1 and extract(
            day from (
                date_trunc(
                    'month',
                    make_date(
                        split_part({{ cleaned_value }}::text, '/', 1)::integer,
                        split_part({{ cleaned_value }}::text, '/', 2)::integer,
                        1
                    ) + interval '1 month'
                ) - interval '1 day'
            )::date
        )
        then make_date(
            split_part({{ cleaned_value }}::text, '/', 1)::integer,
            split_part({{ cleaned_value }}::text, '/', 2)::integer,
            split_part({{ cleaned_value }}::text, '/', 3)::integer
        )

    when {{ cleaned_value }}::text ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$'
        and split_part({{ cleaned_value }}::text, '/', 2)::integer between 1 and 12
        and split_part({{ cleaned_value }}::text, '/', 1)::integer between 1 and extract(
            day from (
                date_trunc(
                    'month',
                    make_date(
                        split_part({{ cleaned_value }}::text, '/', 3)::integer,
                        split_part({{ cleaned_value }}::text, '/', 2)::integer,
                        1
                    ) + interval '1 month'
                ) - interval '1 day'
            )::date
        )
        then make_date(
            split_part({{ cleaned_value }}::text, '/', 3)::integer,
            split_part({{ cleaned_value }}::text, '/', 2)::integer,
            split_part({{ cleaned_value }}::text, '/', 1)::integer
        )

    when {{ cleaned_value }}::text ~ '^\\d{1,2}-\\d{1,2}-\\d{4}$'
        and split_part({{ cleaned_value }}::text, '-', 2)::integer between 1 and 12
        and split_part({{ cleaned_value }}::text, '-', 1)::integer between 1 and extract(
            day from (
                date_trunc(
                    'month',
                    make_date(
                        split_part({{ cleaned_value }}::text, '-', 3)::integer,
                        split_part({{ cleaned_value }}::text, '-', 2)::integer,
                        1
                    ) + interval '1 month'
                ) - interval '1 day'
            )::date
        )
        then make_date(
            split_part({{ cleaned_value }}::text, '-', 3)::integer,
            split_part({{ cleaned_value }}::text, '-', 2)::integer,
            split_part({{ cleaned_value }}::text, '-', 1)::integer
        )

    when {{ cleaned_value }}::text ~ '^\\d{4}-\\d{2}-\\d{2}T'
        and split_part(split_part({{ cleaned_value }}::text, 'T', 1), '-', 2)::integer between 1 and 12
        and split_part(split_part({{ cleaned_value }}::text, 'T', 1), '-', 3)::integer between 1 and extract(
            day from (
                date_trunc(
                    'month',
                    make_date(
                        split_part(split_part({{ cleaned_value }}::text, 'T', 1), '-', 1)::integer,
                        split_part(split_part({{ cleaned_value }}::text, 'T', 1), '-', 2)::integer,
                        1
                    ) + interval '1 month'
                ) - interval '1 day'
            )::date
        )
        then make_date(
            split_part(split_part({{ cleaned_value }}::text, 'T', 1), '-', 1)::integer,
            split_part(split_part({{ cleaned_value }}::text, 'T', 1), '-', 2)::integer,
            split_part(split_part({{ cleaned_value }}::text, 'T', 1), '-', 3)::integer
        )

    else null
end
{%- endmacro %}

{% macro clean_sheet_geography(value_expr) -%}
case
    when {{ clean_sheet_text(value_expr) }} is null then null
    else initcap(
        regexp_replace(
            replace(replace(lower({{ clean_sheet_text(value_expr) }}::text), '_', ' '), '-', ' '),
            '\\s+',
            ' ',
            'g'
        )
    )
end
{%- endmacro %}
