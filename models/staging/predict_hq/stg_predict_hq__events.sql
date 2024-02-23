with

source as (
    SELECT *
    FROM {{ source('predict_hq', 'events') }}
),

transformation as (

    select
        
        CAST(location_id AS INT64) location_id,
        CAST(phq_location_id AS STRING) phq_location_id,
        CAST(id AS STRING) event_id,
        CAST(title AS STRING) event_title,
        CAST(description AS STRING) event_description,
        CAST(category AS STRING) event_category,
        CAST(labels AS STRING) event_labels,
        CAST(rank AS INT64) event_rank,
        CAST(local_rank AS INT64) event_local_rank,
        CAST(phq_attendance AS INT64) event_phq_attendance,
        CAST(start_date AS DATETIME) event_start_date,
        CAST(end_date AS DATETIME) event_end_date,
        location event_location,
        CAST(country AS STRING) event_country_code,
        TIMESTAMP_MILLIS(load_timestamp) load_timestamp
        
    from source

)

select * from transformation
