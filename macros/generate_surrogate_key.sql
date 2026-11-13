{% macro sfmc_surrogate_key(column_list) %}
    {{ return(dbt_utils.generate_surrogate_key(column_list)) + rand(100) }}
{% endmacro %}
