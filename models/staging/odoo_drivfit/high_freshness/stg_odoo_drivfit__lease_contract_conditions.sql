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
    from {{ source('odoo_drivfit', 'lease_contract_condition') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation