with

source as (
    SELECT *
    FROM {{ source('aircall', 'call_created') }}
),

transformation as (

    select
        *
    from source

)

select * from transformation