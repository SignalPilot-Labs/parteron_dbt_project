{% macro create_catalog(catalog_name) %}
    {% set sql %}
        CREATE CATALOG IF NOT EXISTS {{ catalog_name }}
    {% endset %}
    {% do run_query(sql) %}
    {{ log("Catalog " ~ catalog_name ~ " created or already exists", info=True) }}
{% endmacro %}
