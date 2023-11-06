with

source as (
    SELECT *
    FROM {{ source('aircall_v2', 'call_ended') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation