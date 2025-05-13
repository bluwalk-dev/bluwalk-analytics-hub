{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'res_country_city') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation