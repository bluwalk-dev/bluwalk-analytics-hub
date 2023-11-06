with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'sale_order') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted)

    from source

)

select * from transformation