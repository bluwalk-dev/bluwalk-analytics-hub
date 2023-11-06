with

source as (
    SELECT *
    FROM {{ source('aircall', 'call_ringing_on_agent') }}
),

transformation as (

    select
        *
    from source

)

select * from transformation