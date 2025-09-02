WITH ce_banks AS (
  select c.vat, a.sanitized_acc_number
  from {{ ref('stg_odoo__res_partner_banks') }} a 
  left join bluwalk-analytics-hub.staging.stg_odoo_bw_res_partners c on a.partner_id = c.id
)

select 
    b.name, 
    b.vat, 
    c.sanitized_acc_number as odoo_ee_iban, 
    d.sanitized_acc_number as odoo_ce_iban
from {{ ref('stg_odoo_enterprise__account_moves') }} a
left join {{ ref('stg_odoo_enterprise__res_partners') }} b on a.partner_id = b.id
left join {{ ref('stg_odoo_enterprise__res_partner_banks') }} c on a.partner_bank_id = c.id
left join ce_banks d on b.vat = d.vat
where 
  journal_id = 46 and 
  state = 'draft'