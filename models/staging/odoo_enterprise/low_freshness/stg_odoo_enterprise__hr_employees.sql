{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_enterprise', 'hr_employee') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation