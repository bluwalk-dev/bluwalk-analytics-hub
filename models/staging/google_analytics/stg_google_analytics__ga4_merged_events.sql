{{ config(
    materialized='incremental'
) }}

SELECT
    *
FROM
    {{ ref('stg_google_analytics__ga4_events') }}
WHERE event_date BETWEEN '20220925' AND '20240604'