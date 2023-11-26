with

source as (
    select
        *
    from {{ source('odoo_static', 'res_users') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation