WITH account_created_events AS (
    SELECT
        *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number 
        FROM {{ ref('stg_segment__account_created') }}
    ) a
    LEFT JOIN {{ ref('dim_users') }} b ON CAST(a.user_id AS INT64) = b.user_id
    WHERE __row_number = 1
    ORDER BY a.original_timestamp DESC
)

SELECT
  ROUND(UNIX_MICROS(original_timestamp)/ 1000000, 0) as conversion_event_time,
  gclid,
  context_email AS email,
  user_phone AS phone_number,
  1 conversion_value,
  'EUR' currency_code
FROM account_created_events
WHERE 
    gclid IS NOT NULL AND
    original_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 89 DAY)
ORDER BY
    original_timestamp DESC