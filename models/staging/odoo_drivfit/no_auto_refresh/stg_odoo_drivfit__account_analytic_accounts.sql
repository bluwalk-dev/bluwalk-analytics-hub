{{ 
  config(
    materialized='table'
  ) 
}}

with

source as (
    select
        *
    from {{ source('odoo_drivfit', 'account_analytic_account') }}
),

transformation as (

    select

        *

    from source
    
)

select * from transformation