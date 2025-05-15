{{ 
  config(
    materialized='view'
  ) 
}}

with

source as (
    SELECT *
    FROM {{ source('odoo_realtime', 'ir_translation') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation