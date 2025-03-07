{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_enterprise', 'hr_department') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation