with

source as (
    select
        *
    from {{ source('hubspot', 'deal') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted)

    from source

)

select * from transformation