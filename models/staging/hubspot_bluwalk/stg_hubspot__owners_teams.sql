with

source as (
    select
        *
    from {{ source('hubspot', 'owner_team') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced)

    from source

)

select * from transformation