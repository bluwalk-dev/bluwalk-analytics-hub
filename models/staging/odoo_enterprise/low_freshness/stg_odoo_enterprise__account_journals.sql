{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_enterprise', 'account_journal') }}
    where company_id = 4
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ee' || 'account.journal' || id)) as key,
        4 as financial_system_id,
        'odoo_ee' as financial_system,
        *

    from source
    
)

select * from transformation
