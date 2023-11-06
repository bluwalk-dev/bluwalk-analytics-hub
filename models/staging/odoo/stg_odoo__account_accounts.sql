with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'account_account') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted)

    from source

)

select * from transformation