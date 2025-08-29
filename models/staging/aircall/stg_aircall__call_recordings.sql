with

source as (
    SELECT *
    FROM {{ source('aircall_new', 'src_aircall_call_recordings') }}
),

transformation as (

    select
        *
    from source

)

select * from transformation