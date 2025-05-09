with

source as (
    select
        *
    from {{ source('quatenus', 'journal') }}
),

transformation as (

    select
        
        *

    from source
    
)

select * from transformation