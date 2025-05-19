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
    from {{ source('odoo_drivfit', 'purchase_order_line') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation