with

source as (
    select
        *
    from {{ source('odoo_realtime', 'segment') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation