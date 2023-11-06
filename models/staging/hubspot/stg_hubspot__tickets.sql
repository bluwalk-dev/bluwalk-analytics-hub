with

source as (
    select
        *
    from {{ source('hubspot', 'ticket') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_deleted, _fivetran_synced)

    from source

)

select * from transformation