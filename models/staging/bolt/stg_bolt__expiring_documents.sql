WITH

source as (
    select
        *
    from {{ source('bolt', 'expiring_documents') }}
),

transformation as (

    SELECT 
        CAST(expires AS DATE) expiration_date,
        CAST(identifier as STRING) identifier,
        CAST(entity_type AS STRING) entity_type,
        CAST(entity_id AS INT64) entity_id,
        CAST(type_title AS STRING) type_title,
        CAST(company_id AS STRING) company_id,
        TIMESTAMP_MILLIS(load_timestamp) load_timestamp
    FROM source

)

select * from transformation