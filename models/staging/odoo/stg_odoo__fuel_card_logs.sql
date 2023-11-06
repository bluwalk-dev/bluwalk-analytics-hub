with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'fuel_card_log') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted)

    from source

)

select * from transformation