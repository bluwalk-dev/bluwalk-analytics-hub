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
    from {{ source('odoo_bluwalk', 'transaction_category') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation