with

source as (
    select
        *
    from {{ source('odoo_static', 'res_sales_partner_account') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation