{{ 
  config(
    materialized='table'
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