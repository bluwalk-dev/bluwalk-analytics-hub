with

source as (
    select
        *
    from {{ source('odoo_static', 'fleet_vehicle_insurance') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation