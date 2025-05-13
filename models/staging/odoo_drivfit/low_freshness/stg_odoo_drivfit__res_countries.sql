{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'res_country') }}
),

transformation as (

    select
        
        *

    from source
    

)

select * from transformation