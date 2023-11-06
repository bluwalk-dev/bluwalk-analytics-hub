with

source as (
    SELECT *
    FROM {{ source('aircall_v2', 'call_ringing_on_agent') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation