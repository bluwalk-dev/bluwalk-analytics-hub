{{ 
  config(
    materialized='incremental',
    tags=['high_freshness'],
    partition_by={'field': 'write_date', 'data_type': 'timestamp'},
    incremental_strategy='merge',
    unique_key='work_order_id'
  )
}}

with src as (
  select *
  from {{ source('odoo_realtime', 'trips') }}
),

{% if is_incremental() %}

-- 1) Compute watermark over last 7 days using the write_date column directly
watermark as (
  select
    max(write_date) as max_ts
  from {{ this }}
  where date(write_date) >= date_sub(current_date(), interval 7 day)
),

-- 2) Only new/updated rows in that same window
updates as (
  select *
  from src
  where write_date > (select max_ts from watermark)
    and date(write_date) >= date_sub(current_date(), interval 7 day)
)

{% else %}

-- First run: load all rows
updates as (
  select * from src
)

{% endif %}

, transformation as (
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
  from updates
)

select * from transformation
