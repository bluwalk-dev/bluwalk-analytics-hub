with

source as (
    select
        *
    from {{ source('hubspot', 'team_user') }}
),

transformation as (

    select
        
        *

    from source

)

select * from transformation