{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('reporting', 'fct_deals') }}
),

transformation as (

    SELECT
    
        *

    FROM source

)

SELECT * FROM transformation