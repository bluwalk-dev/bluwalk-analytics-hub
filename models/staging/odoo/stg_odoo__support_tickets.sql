with

source as (
    select
        *
    from {{ source('odoo_static', 'support_ticket') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation