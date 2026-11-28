{#
  Removes the pull-request schemas (ci_staging, ci_gold, ci_ml, ci_ops) after a CI run.
  Only allowed on the ci target, so it can never drop dev or prod tables.

    dbt run-operation drop_ci_schemas --target ci
#}
{% macro drop_ci_schemas() %}
    {% if target.name != 'ci' %}
        {{ exceptions.raise_compiler_error("drop_ci_schemas only runs on the ci target") }}
    {% endif %}
    {% for schema in ['ci_staging', 'ci_gold', 'ci_ml', 'ci_ops'] %}
        {% do run_query('DROP SCHEMA IF EXISTS ' ~ target.database ~ '.' ~ schema ~ ' CASCADE') %}
        {{ log('dropped ' ~ target.database ~ '.' ~ schema, info=True) }}
    {% endfor %}
{% endmacro %}
