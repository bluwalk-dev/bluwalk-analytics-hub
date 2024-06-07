{{ config(
    materialized='incremental',
    unique_key=['event_timestamp', 'user_pseudo_id'],
    partition_by={
        'field': 'event_date',
        'data_type': 'DATE'
    }
) }}

SELECT * FROM (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        * EXCEPT(event_date)
    FROM {{ ref('stg_google_analytics__ga342899811_events') }}
    WHERE event_date > '20240605'

    UNION ALL

    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        * EXCEPT(event_date)
    FROM {{ ref('stg_google_analytics__ga323684004_events') }}
    WHERE event_date < '20240605'
)

{% if is_incremental() %}
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
