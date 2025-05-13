{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'rental_contract') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation