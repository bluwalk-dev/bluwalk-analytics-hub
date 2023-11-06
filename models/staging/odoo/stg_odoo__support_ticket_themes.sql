with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'support_ticket_theme') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted)

    from source

)

select * from transformation