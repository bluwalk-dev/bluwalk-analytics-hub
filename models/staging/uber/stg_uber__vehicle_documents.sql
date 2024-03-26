with

source as (
    select
        *
    from {{ source('uber', 'vehicle_compliance') }}
),

transformation as (

    SELECT
        CAST(vehicle_id AS STRING) vehicle_id,
        CAST(vehicle_owner_id AS STRING) vehicle_owner_id,
        CAST(document_global_type_name AS STRING) document_global_type_name,
        CAST(document_type_id AS STRING) document_type_id,
        CAST(document_type_name AS STRING) document_type_name,
        LOWER(CAST(document_status AS STRING)) document_status,
        TIMESTAMP_MILLIS(document_expires_at) document_expires_at,
        TIMESTAMP_MILLIS(load_timestamp) load_timestamp,
    FROM source
    WHERE load_timestamp IS NOT NULL

)

select * from transformation