{% snapshot snap_odoo_statement_close %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='timestamp',
        updated_at='write_date'
    )
}}

select
    id,
    period,
    create_uid,
    create_date,
    write_uid,
    write_date,
    communication_banner,
    invoice_banner,
    segment_com
from {{ ref('stg_odoo__close_periods') }}

{% endsnapshot %}
