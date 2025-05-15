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
    from {{ source('odoo_enterprise', 'account_move_line') }}
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ee' || 'account.move.line' || id)) as key,
        4 as financial_system_id,
        'odoo_ee' as financial_system,
        *

    from source
    
)

select * from transformation
