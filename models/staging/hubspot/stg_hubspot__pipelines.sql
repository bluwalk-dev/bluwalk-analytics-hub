with

source as (
    select
        *
    from {{ source('hubspot', 'pipelines') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation