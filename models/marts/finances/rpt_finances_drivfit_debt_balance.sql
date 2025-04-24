SELECT
  vat,
  sum(amount_residual_signed) as debt
FROM (
  select 
    b.vat, 
    amount_residual_signed 
  from {{ ref('stg_odoo_enterprise__account_moves') }} a
  left join {{ ref('stg_odoo_enterprise__res_partners') }} b ON a.partner_id = b.id
  where journal_id = 104 and partner_id != 1586 and amount_residual_signed > 0 and state = 'posted'

  UNION ALL

  select
    b.vat,
    a.amount_residual_signed
  from {{ ref('stg_odoo_drivfit__account_moves') }} a
  left join {{ ref('stg_odoo_drivfit__res_partners') }} b on a.partner_id = b.id
  where journal_id = 12 and partner_id != 21 and amount_residual_signed > 0 and state = 'posted'
  )
group by vat
