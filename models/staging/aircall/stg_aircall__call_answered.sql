with

source as (
    SELECT *
    FROM {{ source('aircall', 'call_answered') }}
),

transformation as (

    select
        *
    from source

)

select * from transformation