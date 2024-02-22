with

source as (
    SELECT *
    FROM {{ source('predict_hq', 'entities') }}
),

transformation as (

    select
        
        CAST(event_id AS STRING) event_id,
        CAST(entity_id AS STRING) venue_id,
        CAST(name AS STRING) venue_name,
        CAST(type AS STRING) venue_type,
        CAST(formatted_address AS STRING) venue_formatted_address,
        TIMESTAMP_MILLIS(load_timestamp) load_timestamp
        
    from source

)

select * from transformation
