{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

with

source as (
    SELECT *
    FROM {{ source('odoo_bluwalk', 'financial_system') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation