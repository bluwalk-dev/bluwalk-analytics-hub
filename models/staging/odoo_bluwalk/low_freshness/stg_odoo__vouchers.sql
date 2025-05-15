{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'voucher') }}
),

transformation as (

    select
        
        id,
        name,
        expire_date,
        redemption_date,
        redemption_transaction_id,
        redemption_cycle,
        voucher_type_id,
        partner_id,
        currency_id,
        amount,
        is_expired,
        is_redeemed,
        ref,
        create_date,
        write_date

    from source
    
)

select * from transformation