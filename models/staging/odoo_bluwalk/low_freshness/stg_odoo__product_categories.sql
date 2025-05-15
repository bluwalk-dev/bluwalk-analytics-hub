{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

with

source as (
    SELECT *
    FROM {{ source('odoo_realtime', 'product_category') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation