with

source as (
    select
        *
    from {{ source('odoo_realtime', 'fleet_vehicle_model_brand') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation