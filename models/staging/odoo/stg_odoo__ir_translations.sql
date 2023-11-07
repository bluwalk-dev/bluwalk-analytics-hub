with

source as (
    SELECT *
    FROM {{ source('google_cloud_postgresql_public', 'ir_translation') }}
),

transformation as (

    select
        * EXCEPT(_fivetran_synced, _fivetran_deleted)
    from source
    where _fivetran_deleted IS FALSE
)

SELECT * FROM transformation