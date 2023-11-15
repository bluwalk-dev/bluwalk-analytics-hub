with

source as (
    select
        *
    from {{ source('google_cloud_postgresql_public', 'fuel') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_synced, _fivetran_deleted),
        SHA256(CONCAT(
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',CAST(start_date AS TIMESTAMP)),
            card_name,
            CAST(quantity AS NUMERIC))) AS surrogateKey

    from source
    where _fivetran_deleted IS FALSE
)

select * from transformation