{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {# Handle specific cases based on folder names or tags #}
        {% if 'elementary' in node.fqn %}
            {{ target.schema }}_elementary
        {% elif 'intermediate' in node.fqn %}
            {{ target.schema }}_staging

        {# Default case: use dynamic folder-based naming #}
        {% elif 'marts' in node.fqn and node.fqn.index('marts') + 1 < node.fqn | length %}
            {% set prefix = node.fqn[node.fqn.index('marts') + 1] %}
            {{ target.schema }}_{{ prefix | trim }}
        {% else %}
            {{ default_schema }}
        {% endif %}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}