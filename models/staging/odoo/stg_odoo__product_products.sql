with

source as (
    SELECT *
    FROM {{ source('google_cloud_postgresql_public', 'product_product') }}
),

transformation as (

    select
        * EXCEPT(_fivetran_synced, _fivetran_deleted)
    from source

)

SELECT * FROM transformation
where _fivetran_deleted IS FALSE