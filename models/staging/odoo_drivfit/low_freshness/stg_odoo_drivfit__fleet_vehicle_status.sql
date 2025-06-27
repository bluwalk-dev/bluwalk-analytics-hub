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
    from {{ source('odoo_drivfit', 'fleet_vehicle_status') }}
),

transformation as (

    select

        id,
        status,
        vehicle_id,
        movement_number,
        station_id,
        CAST(date AS TIMESTAMP) as mov_ts,
        DATETIME(CAST(date AS TIMESTAMP), 'Europe/Lisbon') as mov_dt

    from source
    
)

select * from transformation