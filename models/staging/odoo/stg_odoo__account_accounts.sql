with

source as (
    select
        *
    from {{ source('odoo_static', 'account_account') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation