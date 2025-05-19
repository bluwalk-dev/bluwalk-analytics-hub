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
    from {{ source('odoo_drivfit', 'product_template') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation