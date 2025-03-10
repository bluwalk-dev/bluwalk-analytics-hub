{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'transaction_account') }}
),

transformation as (

    select
        
        id as transaction_account_id,
        name as transaction_account_name,
        user_id,
        partner_email as contact_email,
        partner_nif as contact_vat,
        type as transaction_account_type,

    from source
    
)

select * from transformation