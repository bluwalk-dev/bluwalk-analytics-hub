with

source as (
    select
        *
    from {{ source('generic', 'close_period_log') }}
),

transformation as (

    SELECT
    
        CAST(date AS DATE) date,
        CAST(period AS INT64) statement,
        CAST(lastUpdate AS TIMESTAMP) last_update_timestamp,
        DATETIME(TIMESTAMP(lastUpdate), 'Europe/Lisbon') as last_update_localtime

    FROM source

)

SELECT * FROM transformation

