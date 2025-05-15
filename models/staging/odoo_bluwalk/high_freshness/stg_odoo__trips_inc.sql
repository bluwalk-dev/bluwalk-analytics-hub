{{
  config(
    materialized='incremental',
    tags=['high_freshness'],
    partition_by={'field': 'write_date', 'data_type': 'timestamp'},
    incremental_strategy='merge',
    unique_key='work_order_id'
  )
}}

with source as (
    select *
    from {{ source('odoo_realtime', 'trips') }}
),

filtered as (
    {% if is_incremental() %}
      -- only grab rows created/updated since last run
      select *
      from source
      where write_date > (
        select max(write_date)
        from {{ this }}
      )
    {% else %}
      -- first run: grab everything
      select * from source
    {% endif %}
),

transformation as (
    select
        id                         as work_order_id,
        name                       as work_order_name,
        status                     as work_order_status,
        transaction_hash           as work_order_hash,
        payment_cycle              as statement,
        billing_account_id,
        partner_id                 as contact_id,
        sales_partner_id,
        sales_partner_driver       as user_partner_uuid,
        sales_account_city         as location,
        sales_segment              as stream,
        period_start               as start_date,
        period_end                 as end_date,
        gross_sales                as sales_gross,
        net_sales                  as sales_net,
        sales_taxes,
        sales_tax_rate,
        gross_partner_fee          as partner_fee_gross,
        net_partner_fee            as partner_fee_net,
        partner_fee_taxes,
        partner_fee_tax_rate,
        partner_payment            as partner_payout,
        create_uid,
        create_date,
        write_uid,
        write_date,
        res_sales_partner_id,
        nr_trips
    from filtered
)

select * from transformation
