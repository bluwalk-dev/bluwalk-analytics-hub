{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'sale_order') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation