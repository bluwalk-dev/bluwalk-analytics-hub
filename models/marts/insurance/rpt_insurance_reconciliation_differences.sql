WITH odoo_policies AS (
  SELECT
    policy_number, 
    c.name AS insurer_name, 
    state, 
    SUM(a.amount) AS amount
  FROM {{ ref("stg_odoo__insurance_policy_payments") }} a
  LEFT JOIN {{ ref("stg_odoo__insurance_policies") }} b
    ON a.policy_id = b.id
  LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_bw_res_partners c
    ON b.insurer_id = c.id
  WHERE a.start_date < (
    SELECT DATE_TRUNC(
      DATE_ADD(PARSE_DATE('%Y%m', CAST(MAX(year_month) AS STRING)), INTERVAL 1 MONTH),
      MONTH
    )
    FROM {{ ref("base_mds_reconciliation") }}
  )
  GROUP BY policy_number, state, c.name
),

mds_policies_deduplicated AS (
  SELECT 
    REGEXP_REPLACE(CAST(apolice AS STRING), r'^0+', '') AS clean_policy_number,
    MAX(segurador) AS segurador,
    MAX(cliente) AS cliente,
    SUM(premio_total) AS amount
  FROM {{ ref("base_mds_reconciliation") }}
  GROUP BY clean_policy_number
)

SELECT
  a.policy_number,
  a.insurer_name,
  a.amount AS bw_amount,
  COALESCE(b.amount, 0) AS mds_amount,
  a.amount - COALESCE(b.amount, 0) AS diff
FROM odoo_policies a
LEFT JOIN mds_policies_deduplicated b
  ON REGEXP_REPLACE(CAST(a.policy_number AS STRING), r'^0+', '') = b.clean_policy_number
ORDER BY a.policy_number ASC