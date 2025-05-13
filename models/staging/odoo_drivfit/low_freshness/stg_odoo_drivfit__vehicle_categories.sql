{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'vehicle_category') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation