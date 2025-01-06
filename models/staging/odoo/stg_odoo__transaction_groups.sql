{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'transaction_group') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation