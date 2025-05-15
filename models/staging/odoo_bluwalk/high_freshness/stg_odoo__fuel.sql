{{ 
  config(
    materialized='table',
    tags=['high_freshness']
  ) 
}}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'fuel') }}
),

transformation as (

    select
        *
    FROM (
        select
            id energy_id,
            name,
            card_name,
            CAST(start_date AS TIMESTAMP) start_date_ts,
            DATETIME(CAST(start_date AS TIMESTAMP), 'Europe/Lisbon') start_date,
            CAST(end_date AS TIMESTAMP) end_date_ts,
            DATETIME(CAST(end_date AS TIMESTAMP), 'Europe/Lisbon') end_date,
            station_name,
            station_type,
            fuel_source AS energy_source,
            partner_id AS contact_id,
            product,
            ROUND(quantity,2) AS quantity,
            measurement_unit,
            ROUND(cost,2) AS cost,
            ROUND(margin,2) AS margin,
            payment_cycle AS statement,
            status,
            supplier_id AS supplier_contact_id,
            res_service_partner_id AS service_partner_id,
            create_date,
            create_uid,
            write_date,
            write_uid,
            ba_credit,
            ba_credit_product_id,
            ba_debit,
            ba_debit_product_id,
            billing_account_id,
            company_id

        from source
        
    )
)

select 
    TO_HEX(MD5(end_date_ts || card_name || quantity)) as transaction_key,
    *
from transformation