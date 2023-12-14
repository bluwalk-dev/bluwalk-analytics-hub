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
      LAG(localDateTime) OVER (PARTITION BY org_alt_name ORDER BY localDateTime) AS start_period,
      localDateTime end_period
    FROM {{ ref('stg_uber__payment_orders') }}
    WHERE transaction_description = 'so.payout'
  ) a
  LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.start_period AS DATE) = b.date
)

SELECT
  a.*,
  b.year_week
FROM {{ ref('stg_uber__payment_orders') }} a
JOIN periods b ON a.org_alt_name = b.org_alt_name
WHERE a.localDateTime > b.start_period AND a.localDateTime <= b.end_period
ORDER BY a.localDateTime DESC