WITH first_payments AS (
  SELECT
    insurance_policy_id AS policy_id,
    amount,
    ROW_NUMBER() OVER (
      PARTITION BY insurance_policy_id
      ORDER BY start_date
    )                            AS rn
  FROM {{ ref('fct_insurance_policy_payments') }}
)

SELECT
  d.close_date_month as year_month,
  d.owner_name as agent_name,
  SUM(COALESCE(fp.amount, 0)) AS first_payment_total
FROM {{ ref('fct_deals') }}       AS d
JOIN {{ ref('dim_insurance_policies') }} AS p
  ON d.insurance_policy_name = p.insurance_policy_name
LEFT JOIN first_payments         AS fp
  ON p.insurance_policy_id = fp.policy_id
  AND fp.rn = 1
WHERE
  d.deal_pipeline_id = 'default'
  AND d.insurance_policy_name IS NOT NULL
GROUP BY
  d.close_date_month,
  d.owner_name
ORDER BY
  d.close_date_month DESC