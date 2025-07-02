WITH enterprise_deposits AS (
  SELECT
    p.vat,
    p.name,
    p.email,
    COALESCE(p.mobile, p.phone) AS phone,
    -aml.balance             AS deposit_amount
  FROM {{ ref('stg_odoo_enterprise__account_move_lines') }} aml
  JOIN {{ ref('stg_odoo_enterprise__account_accounts') }} acct
    ON aml.account_id = acct.id
  JOIN {{ ref('stg_odoo_enterprise__res_partners') }} p
    ON aml.partner_id = p.id
  WHERE
    aml.journal_id = 132
    AND acct.code LIKE '278%'
),

community_deposits AS (
  SELECT
    p.vat,
    p.name,
    p.email,
    COALESCE(p.mobile, p.phone) AS phone,
    aml.amount_untaxed_signed AS deposit_amount
  FROM {{ ref('stg_odoo_drivfit__account_moves') }} aml
  JOIN {{ ref('stg_odoo_drivfit__res_partners') }} p
    ON aml.partner_id = p.id
  WHERE
    aml.state = 'posted'
    AND aml.type IN ('out_deposit','out_refund_deposit')
    AND aml.date < '2025-01-01'
),

all_deposits AS (
  SELECT * FROM enterprise_deposits
  UNION ALL
  SELECT * FROM community_deposits
)

SELECT
  vat,
  name,
  email,
  phone,
  SUM(deposit_amount) AS current_deposits
FROM all_deposits
GROUP BY
  vat, name, email, phone