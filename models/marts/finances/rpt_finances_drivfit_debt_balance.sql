SELECT
  vat,
  sum(amount_residual_signed) as debt
FROM (
  SELECT 
    b.accounting_contact_vat as vat, 
    amount_residual_signed 
  from bluwalk-analytics-hub.staging.stg_odoo_ee_account_moves a
  left join bluwalk-analytics-hub.core.core_contacts_ee b ON a.partner_id = b.accounting_contact_id
  where journal_id = 104 and partner_id != 1586 and amount_residual_signed > 0 and state = 'posted'

  UNION ALL

  select
    b.contact_vat as vat,
    a.amount_residual_signed
  from {{ ref('stg_odoo_drivfit__account_moves') }} a
  left join bluwalk-analytics-hub.core.core_contacts_flt b on a.partner_id = b.contact_id
  where a.journal_id = 12 and a.partner_id != 21 and a.amount_residual_signed > 0 and a.state = 'posted'
  )
group by vat
