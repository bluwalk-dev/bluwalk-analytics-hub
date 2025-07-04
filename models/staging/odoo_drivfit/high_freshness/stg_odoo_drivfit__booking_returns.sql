{{ 
  config(
    materialized='table',
    tags=['high_freshness']
  ) 
}}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'booking_return') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation