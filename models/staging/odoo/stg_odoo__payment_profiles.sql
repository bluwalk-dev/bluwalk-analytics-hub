{{ config(materialized='table') }}

with

source as (
    SELECT *
    FROM {{ source('odoo_realtime', 'payment_profile') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation