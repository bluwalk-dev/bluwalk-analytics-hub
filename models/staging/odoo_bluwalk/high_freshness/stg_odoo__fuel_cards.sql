{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'fuel_card') }}
),

transformation as (

    select
        *
    FROM source
)

select * from transformation