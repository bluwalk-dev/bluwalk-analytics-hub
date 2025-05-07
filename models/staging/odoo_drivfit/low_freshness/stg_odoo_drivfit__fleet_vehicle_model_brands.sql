{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'fleet_vehicle_model_brand') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation