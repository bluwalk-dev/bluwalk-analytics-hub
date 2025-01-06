{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'transaction_line') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation