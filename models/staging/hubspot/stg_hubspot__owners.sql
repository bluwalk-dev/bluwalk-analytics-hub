with

source as (
    select
        *
    from {{ source('hubspot', 'owner') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced)

    from source

)

select * from transformation