with

source as (
    select
        *
    from {{ source('odoo_static', 'account_analytic_account') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation