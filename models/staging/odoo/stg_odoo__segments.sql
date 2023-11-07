with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'segment') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted)

    from source
    where _fivetran_deleted IS FALSE
)

select * from transformation