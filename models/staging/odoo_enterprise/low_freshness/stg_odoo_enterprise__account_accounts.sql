{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_enterprise', 'account_account') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation