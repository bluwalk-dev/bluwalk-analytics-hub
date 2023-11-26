with

source as (
    select
        *
    from {{ source('odoo_static', 'rate_base') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation