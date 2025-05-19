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
    from {{ source('odoo_bluwalk', 'account_journal') }}
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ce' || 'account.journal' || id)) as key,
        1 as financial_system_id,
        'odoo_ce' as financial_system,
        *

    from source
    
)

select * from transformation