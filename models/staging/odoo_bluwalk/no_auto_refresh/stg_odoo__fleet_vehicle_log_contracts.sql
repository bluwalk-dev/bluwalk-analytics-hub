{{ 
  config(
    materialized='table'
  ) 
}}

with

source as (
    select
        *
    from {{ source('odoo_bluwalk', 'fleet_vehicle_log_contract') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation