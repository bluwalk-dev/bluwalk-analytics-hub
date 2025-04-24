{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'account_move_line') }}
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ce' || 'account.move.line' || id)) as key,
        1 as financial_system_id,
        'odoo_ce' as financial_system,
        *

    from source
    
)

select * from transformation