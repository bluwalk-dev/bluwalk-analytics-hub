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
    from {{ source('odoo_bluwalk', 'fleet_vehicle_model') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation