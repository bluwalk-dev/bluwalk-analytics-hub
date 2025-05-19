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
    from {{ source('odoo_bluwalk', 'support_ticket') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation