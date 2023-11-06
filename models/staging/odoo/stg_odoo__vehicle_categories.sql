with

source as (
    select
        *
    from {{ source('odoo_realtime', 'vehicle_category') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation