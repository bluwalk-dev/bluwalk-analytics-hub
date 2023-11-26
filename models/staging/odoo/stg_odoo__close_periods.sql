with

source as (
    select
        *
    from {{ source('odoo_static', 'close_period') }}
),

transformation as (

    select
        
        *

    from source
    

)

select * from transformation