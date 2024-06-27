with

source as (
    SELECT DISTINCT
        *
    FROM {{ source('bp', 'cards') }}
),

transformation as (

    select
        *
    from source

)

select * from transformation