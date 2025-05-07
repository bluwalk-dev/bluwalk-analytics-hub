with

source as (
    select
        *
    from {{ source('hubspot', 'users') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation