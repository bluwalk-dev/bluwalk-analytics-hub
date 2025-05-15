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
    from {{ source('odoo_realtime', 'res_service_partner_type') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation