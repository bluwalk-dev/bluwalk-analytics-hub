with

source as (
    SELECT *
    FROM {{ source('aircall_v3', 'call') }}
),

transformation as (

    select
        *
    from source

)

select * from transformation