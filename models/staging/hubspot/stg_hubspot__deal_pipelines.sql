with

source as (
    select
        *
    from {{ source('hubspot', 'deal_pipeline') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted)

    from source

)

select * from transformation