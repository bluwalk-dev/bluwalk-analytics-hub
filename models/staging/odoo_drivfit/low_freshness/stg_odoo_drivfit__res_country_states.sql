{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'res_country_state') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation