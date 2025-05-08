{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'billable_item') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation