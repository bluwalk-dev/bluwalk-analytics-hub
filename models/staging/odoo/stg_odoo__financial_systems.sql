{{ config(materialized='table') }}

with

source as (
    SELECT *
    FROM {{ source('odoo_realtime', 'financial_system') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation