{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'fuel_card_log') }}
),

transformation as (

    select
        *
    FROM source
)

select * from transformation