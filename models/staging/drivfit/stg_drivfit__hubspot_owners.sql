{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('drivfit_hubspot', 'owner') }}
),

transformation as (

    SELECT
    
        *

    FROM source

)

SELECT * FROM transformation