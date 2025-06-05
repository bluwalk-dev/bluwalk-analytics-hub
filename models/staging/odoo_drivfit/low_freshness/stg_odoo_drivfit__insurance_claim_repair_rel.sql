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
    from {{ source('odoo_drivfit', 'insurance_claim_repair_rel') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation