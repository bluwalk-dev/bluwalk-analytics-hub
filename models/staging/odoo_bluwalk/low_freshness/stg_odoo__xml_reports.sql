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
    from {{ source('odoo_realtime', 'ir_act_report_xml') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation