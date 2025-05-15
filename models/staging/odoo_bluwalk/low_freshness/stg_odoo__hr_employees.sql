{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

with

source as (
    SELECT *
    FROM {{ source('odoo_realtime', 'hr_employee') }}
),

transformation as (

    select
        *
    from source
    

)

SELECT * FROM transformation