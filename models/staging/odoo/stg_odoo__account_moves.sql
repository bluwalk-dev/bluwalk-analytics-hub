with

source as (
    select
        *
    from {{ source('odoo_static', 'account_move') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation