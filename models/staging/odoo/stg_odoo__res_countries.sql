with

source as (
    select
        *
    from {{ source('odoo_static', 'res_country') }}
),

transformation as (

    select
        
        *

    from source
    

)

select * from transformation