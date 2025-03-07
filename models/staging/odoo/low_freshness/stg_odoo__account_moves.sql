{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'account_move') }}
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ce' || 'account.move' || id)) as key,
        'odoo_ce' as financial_system,
        *

    from source
    
)

select * from transformation