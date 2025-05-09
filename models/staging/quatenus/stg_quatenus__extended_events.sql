with

source as (
    select
        *
    from {{ source('quatenus', 'extendedEvents') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation