with

source as (
    SELECT *
    FROM {{ source('aircall_v2', 'call_recording') }}
),

transformation as (

    select
        *
    from source

)

select * from transformation