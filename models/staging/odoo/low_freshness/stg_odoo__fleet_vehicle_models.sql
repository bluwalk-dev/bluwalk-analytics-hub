{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'fleet_vehicle_model') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation