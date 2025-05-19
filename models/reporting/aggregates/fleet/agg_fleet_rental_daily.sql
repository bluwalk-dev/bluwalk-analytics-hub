WITH cutoff AS (
  SELECT
    DATE_TRUNC( 
      DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),  -- always “yesterday”
      WEEK                                        -- week starting Sunday
    ) AS last_full_week_start
),

rentals AS (
  SELECT
    r.date,
    c.year_month,
    c.year_week,
    r.customer_type,
    r.total_days
  FROM {{ ref('int_fleet_rentals_per_day_list') }} AS r
  LEFT JOIN {{ ref('util_calendar') }} AS c
    ON r.date = c.date
)

-- 3) Filter, aggregate, round:
SELECT
  r.date,
  r.year_month,
  r.year_week,

  -- SUM raw values, then ROUND once:
  ROUND( SUM( IF(r.customer_type = 'driver',  r.total_days, 0) ), 0 )
    AS driver_rental_days,

  ROUND( SUM( IF(r.customer_type = 'company', r.total_days, 0) ), 0 )
    AS company_rental_days,

  ROUND( SUM(r.total_days), 0 )                AS rental_days

FROM rentals AS r
CROSS JOIN cutoff AS cf
WHERE r.date <= cf.last_full_week_start

GROUP BY
  r.date,
  r.year_month,
  r.year_week

ORDER BY
  r.date DESC