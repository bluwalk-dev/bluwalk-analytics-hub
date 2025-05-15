{{ 
  config(
    materialized='table',
    tags=['high_freshness']
  ) 
}}

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