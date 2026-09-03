{#
    Default dbt behaviour is to prefix custom schemas with the target schema
    (`staging_marts`). We want the schema names from dbt_project.yml exactly as
    written in dev and prod, but the prefixed behaviour in CI so every run is
    isolated in its own throwaway schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'ci' -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
