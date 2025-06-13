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
    from {{ source('odoo_drivfit', 'fleet_vehicle_log_services') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation