{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

with

source as (
    SELECT *
    FROM {{ source('odoo_bluwalk', 'hr_department') }}
),

transformation as (

    select
        *
    from source
    

)

SELECT * FROM transformation
