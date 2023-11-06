with

source as (
    select
        *
    from {{ source('hubspot', 'merged_deal') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced)

    from source

)

select * from transformation