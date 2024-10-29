{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('mercadao', 'availability') }} 
),

transformation as (

    SELECT
    
        location,
        schedule,
        CASE
            WHEN status = 'TRUE' THEN TRUE
            ELSE FALSE
        END as status

    FROM source

)

SELECT * FROM transformation
WHERE status = TRUE