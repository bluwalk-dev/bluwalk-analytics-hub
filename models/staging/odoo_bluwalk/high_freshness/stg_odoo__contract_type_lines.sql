{{ 
  config(
    materialized='table',
    tags=['high_freshness']
  ) 
}}

with

source as (
    select
        *
    from {{ source('odoo_realtime', 'contract_type_line') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation