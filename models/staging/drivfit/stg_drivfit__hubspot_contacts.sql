{{ config(
    materialized='table',
    enabled = false
) }}

with

source as (
    select
        *
    from {{ source('drivfit_hubspot', 'contact') }}
),

transformation as (

    SELECT
    
        *

    FROM source
    WHERE _fivetran_deleted IS FALSE

)

SELECT * FROM transformation