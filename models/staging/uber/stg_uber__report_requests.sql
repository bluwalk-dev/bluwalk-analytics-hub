with

source as (
    select
        *
    from {{ source('uber_v2', 'src_uber_report_requests') }}
),

transformation as (

    SELECT
        org_id,
        report_id,
        report_type,
        start_utc,
        DATETIME(TIMESTAMP(start_utc), 'Europe/Lisbon') as start_localtime,
        end_utc,
        DATETIME(TIMESTAMP(end_utc), 'Europe/Lisbon') as end_local_time,
        status,
        file_name,
        request_timestamp,
        processed_timestamp
    FROM source

)

select * from transformation