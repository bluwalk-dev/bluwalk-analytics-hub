with

source as (
    select
        *
    from {{ source('odoo_static', 'fleet_vehicle') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation