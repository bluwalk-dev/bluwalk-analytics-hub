{{ config(materialized='table') }}

with

source as (
    SELECT *
    FROM {{ source('odoo_realtime', 'product_template') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation