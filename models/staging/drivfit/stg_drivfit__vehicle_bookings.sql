{{ config(
    materialized='table',
    enabled = false
) }}

WITH

source as (
    select
        *
    from {{ source('reporting', 'fct_rental_bookings') }}
),

transformation as (

    SELECT
    
        *

    FROM source
    WHERE customer_id = 21

)

SELECT * FROM transformation