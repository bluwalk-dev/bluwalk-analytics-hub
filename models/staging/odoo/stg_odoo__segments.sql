with

source as (
    select
        *
    from {{ source('odoo_static', 'segment') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation