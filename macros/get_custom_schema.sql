{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if target.name == 'dev' -%}
        {#- in dev, prefix with the invoking user so multiple devs don't collide -#}
        {{ default_schema }}_{{ custom_schema_name | trim }}_{{ env_var('USER', 'local') }}
    {%- elif custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
