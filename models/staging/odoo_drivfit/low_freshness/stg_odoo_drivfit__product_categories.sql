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
    from {{ source('odoo_drivfit', 'product_category') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation