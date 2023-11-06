{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set env_schema_prefix = "" -%}

    {#- Adjust the prefix based on the target name or an environment variable -#}
    {%- if target.name == 'prod' -%}
        {#- In production, don't use a prefix -#}
    {%- else -%}
        {#- In other environments, use the 'dbt-dev-' prefix -#}
        {%- set env_schema_prefix = "dbt_dev_" -%}
    {%- endif -%}

    {#- If a custom schema is specified, use it; otherwise, use the target schema -#}
    {%- if custom_schema_name is none -%}
        {{ env_schema_prefix }}{{ target.schema }}
    {%- else -%}
        {{ env_schema_prefix }}{{ custom_schema_name }}
    {%- endif -%}

{%- endmacro %}