{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'fleet_vehicle') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation