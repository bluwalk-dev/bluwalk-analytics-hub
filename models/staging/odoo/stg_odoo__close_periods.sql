{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'close_period') }}
),

transformation as (

    select
        
        *

    from source
    

)

select * from transformation