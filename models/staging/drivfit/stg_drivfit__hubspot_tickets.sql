{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('drivfit_hubspot', 'ticket') }}
),

transformation as (

    SELECT
    
        * EXCEPT(_fivetran_deleted, _fivetran_synced)

    FROM source
    WHERE _fivetran_deleted IS FALSE

)

SELECT * FROM transformation