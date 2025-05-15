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
    from {{ source('odoo_realtime', 'account_analytic_account') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation