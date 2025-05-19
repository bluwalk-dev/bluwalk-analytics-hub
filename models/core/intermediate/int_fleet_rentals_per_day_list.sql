WITH item_days AS (
  SELECT
    c.date,
    t.contract_id,
    t.contract_type,
    t.contract_name,

    CASE 
      WHEN t.customer_id = 21 THEN 'driver'
      ELSE 'company'
    END AS customer_type,

    -- compute the signed “day-delta” for each calendar date
    ( CASE WHEN t.amount > 0 THEN 1 ELSE -1 END )
    *
    ( CASE 
        WHEN t.contract_type = 'short-term' 
          THEN t.quantity / 24.0     -- ensure float division
        ELSE 1 
      END
    )               AS day_delta
  FROM {{ ref('fct_fleet_billable_items') }} t
  JOIN {{ ref('util_calendar') }} c
    ON c.date BETWEEN t.period_start AND t.period_end
  WHERE t.product_id = 323
)

SELECT
  date,
  contract_id,
  contract_type,
  contract_name,
  customer_type,

  -- round only once, after aggregation
  ROUND(SUM(day_delta), 2) AS total_days

FROM item_days

GROUP BY
  date,
  contract_id,
  contract_type,
  contract_name,
  customer_type

-- filter on the raw sum, which avoids double ROUND() calls
HAVING SUM(day_delta) > 0

ORDER BY
  date