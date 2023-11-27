{% macro table_exists(table_name, dataset) %}
{%- if execute -%}
    {%- set full_table_name = generate_schema_name(dataset, none) ~ '.' ~ table_name -%}
    {{ log("Full table name: " ~ full_table_name, info=True) }}
    {%- set table_exists_query -%}
        SELECT count(*)
        FROM `{{ target.project }}.{{ generate_schema_name(dataset, none) }}.INFORMATION_SCHEMA.TABLES`
        WHERE CONCAT(table_schema, '.', table_name) = '{{ full_table_name }}'
    {%- endset -%}

    {%- set results = run_query(table_exists_query) -%}
    {%- if results and results.table and results.table.rows[0][0] | int > 0 -%}
        {%- set exists = true -%}
    {%- else -%}
        {%- set exists = false -%}
    {%- endif -%}
{%- else -%}
    {%- set exists = true -%}  -- Assume the table exists in a non-execution context
{%- endif -%}

{{ return(exists) }}
{% endmacro %}