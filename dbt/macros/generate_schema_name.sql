{#
  Use the schema set in dbt_project.yml as is (staging, gold, ml) instead of dbt's
  default "<target schema>_<custom schema>". The catalog (gridpulse_dev or
  gridpulse_prod) already separates environments.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
