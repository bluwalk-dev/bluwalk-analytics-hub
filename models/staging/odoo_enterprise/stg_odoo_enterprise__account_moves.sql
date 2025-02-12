{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_enterprise', 'account_move') }}
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ee' || 'account.move' || id)) as key,
        'odoo_ee' as financial_system,
        *

    from source
    where company_id = 4
    
)

select * from transformation
