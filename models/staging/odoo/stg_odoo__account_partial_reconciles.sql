with

source as (
    select
        *
    from {{ source('odoo_static', 'account_partial_reconcile') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation