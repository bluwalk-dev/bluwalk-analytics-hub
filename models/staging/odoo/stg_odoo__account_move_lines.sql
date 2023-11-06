with

source as (
    select
        *
    from {{ source('odoo_realtime', 'account_move_line') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation