{{ config(
    materialized='incremental',
    unique_key=['event_timestamp', 'user_pseudo_id'],
    partition_by={
        'field': 'event_date_parsed',
        'data_type': 'DATE'
    }
) }}

SELECT
    *,
    PARSE_DATE('%Y%m%d', event_date) AS event_date_parsed  -- Ensure event_date is parsed to DATE
FROM
    {{ ref('base_google_analytics_4_page_views') }}

{% if is_incremental() %}
WHERE
    event_date > (SELECT MAX(event_date) FROM {{ this }})
{% endif %}