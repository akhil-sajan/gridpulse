{#
  Use the schema set in dbt_project.yml as is (staging, gold, ml) instead of dbt's
  default "<target schema>_<custom schema>". The catalog (gridpulse_dev or
  gridpulse_prod) already separates environments.

  Exception, target "ci" (pull requests, P5.4): models go to ci_staging, ci_gold, ci_ml
  in gridpulse_dev, so a PR never overwrites the dev tables. drop_ci_schemas() removes
  them after the run.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set name = target.schema if custom_schema_name is none else custom_schema_name | trim -%}
    {%- if target.name == 'ci' and not name.startswith('ci_') -%}
        ci_{{ name }}
    {%- else -%}
        {{ name }}
    {%- endif -%}
{%- endmacro %}
