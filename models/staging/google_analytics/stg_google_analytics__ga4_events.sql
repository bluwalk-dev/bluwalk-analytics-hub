{{ config(
    materialized='incremental',
    unique_key=['event_timestamp', 'user_pseudo_id'],
    partition_by={
        'field': 'event_date',
        'data_type': 'DATE'
    }
) }}

WITH base AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        * EXCEPT(event_date)
    FROM {{ ref('stg_google_analytics__ga4_events_t') }}
    WHERE event_date = '20240605'
)

SELECT
    *
FROM base

{% if is_incremental() %}
WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
