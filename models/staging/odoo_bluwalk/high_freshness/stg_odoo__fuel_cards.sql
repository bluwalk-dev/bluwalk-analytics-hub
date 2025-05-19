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
    from {{ source('odoo_bluwalk', 'fuel_card') }}
),

transformation as (

    select
        *
    FROM source
)

select * from transformation