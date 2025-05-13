{{ config(
    enabled = false
) }}

{% snapshot snap_employees_history %}

{{
    config(
        target_schema='snapshots',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    *
from {{ ref('dim_employees') }}

{% endsnapshot %}
