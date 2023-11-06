with

source as (
    select
        *
    from {{ source('hubspot', 'team') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced)

    from source

)

select * from transformation