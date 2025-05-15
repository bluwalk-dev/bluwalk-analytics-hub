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
    from {{ source('odoo_enterprise', 'account_account') }}
),

transformation as (

    select
        
        TO_HEX(MD5('odoo_ee' || 'account.account' || id)) as key,
        4 as financial_system_id,
        'odoo_ee' as financial_system,
        *

    from source
    
)

select * from transformation