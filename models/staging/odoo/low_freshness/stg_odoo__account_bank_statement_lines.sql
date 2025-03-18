{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'account_bank_statement_line') }}
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ee' || 'account.bank.statement.line' || id)) as key,
        'odoo_ce' as financial_system,
        *

    from source
    
)

select * from transformation
