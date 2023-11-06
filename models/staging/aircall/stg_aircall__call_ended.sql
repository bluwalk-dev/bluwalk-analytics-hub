with

source as (
    SELECT *
    FROM {{ source('aircall', 'call_ended') }}
),

transformation as (

    select
        *
    from source

)

select * from transformation