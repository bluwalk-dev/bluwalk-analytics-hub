{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'account_partial_reconcile') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation