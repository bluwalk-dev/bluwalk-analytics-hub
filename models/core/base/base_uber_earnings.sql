WITH periods AS (
  SELECT
    a.org_alt_name,
    a.start_period,
    a.end_period,
    b.year_week
  FROM (
    SELECT 
      transaction_uuid,
      org_alt_name,
      LAG(local_datetime) OVER (PARTITION BY org_alt_name ORDER BY local_datetime) AS start_period,
      local_datetime end_period
    FROM {{ ref('stg_uber__payment_orders') }}
    WHERE transaction_description = 'so.payout'
  ) a
  LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.start_period AS DATE) = b.date
), last_version_payment_orders AS (
    SELECT * EXCEPT(__row_number) FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY transaction_uuid 
                ORDER BY load_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_uber__payment_orders") }} 
        )
    WHERE __row_number = 1
)

SELECT
  a.*,
  b.year_week
FROM last_version_payment_orders a
JOIN periods b ON a.org_alt_name = b.org_alt_name
WHERE a.local_datetime > b.start_period AND a.local_datetime <= b.end_period
ORDER BY a.local_datetime DESC