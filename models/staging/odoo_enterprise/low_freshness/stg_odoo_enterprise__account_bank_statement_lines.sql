{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_enterprise', 'account_bank_statement_line') }}
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ee' || 'account.bank.statement.line' || id)) as key,
        'odoo_ee' as financial_system,
        *

    from source
    
)

select * from transformation
