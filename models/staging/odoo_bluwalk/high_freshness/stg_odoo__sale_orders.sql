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
    from {{ source('odoo_bluwalk', 'sale_order') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation