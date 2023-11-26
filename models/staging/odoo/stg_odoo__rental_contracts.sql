with

source as (
    select
        *
    from {{ source('odoo_static', 'rental_contract') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation