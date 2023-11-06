with

source as (
    select
        *
    from {{ source('hubspot', 'contact') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted)

    from source

)

select * from transformation
