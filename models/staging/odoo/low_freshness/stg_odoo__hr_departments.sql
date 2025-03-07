{{ config(materialized='table') }}

with

source as (
    SELECT *
    FROM {{ source('odoo_realtime', 'hr_department') }}
),

transformation as (

    select
        *
    from source
    

)

SELECT * FROM transformation
