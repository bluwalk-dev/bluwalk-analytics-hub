{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_enterprise', 'res_company') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation