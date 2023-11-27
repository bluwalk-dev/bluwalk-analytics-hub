{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'res_sales_partner_type') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation