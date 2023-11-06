with

source as (
    select
        *
    from {{ source('odoo_realtime', 'account_payment') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation