{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

with

source as (
    select
        *
    from {{ source('odoo_enterprise', 'hr_department') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation