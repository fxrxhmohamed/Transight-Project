{#- Models go to the schema they name (metrics, marts), not
    <target schema>_<schema> as dbt does by default. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
