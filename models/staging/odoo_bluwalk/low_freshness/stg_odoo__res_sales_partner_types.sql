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
    from {{ source('odoo_bluwalk', 'res_sales_partner_type') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation