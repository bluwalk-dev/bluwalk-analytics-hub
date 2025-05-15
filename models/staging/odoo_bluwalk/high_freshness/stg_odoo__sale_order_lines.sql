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
    from {{ source('odoo_realtime', 'sale_order_line') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation