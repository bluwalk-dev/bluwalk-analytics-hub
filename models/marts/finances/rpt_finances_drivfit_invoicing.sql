select
	date as data,
	name as numero_fatura,
	invoice_partner_display_name as cliente,
	payment_state as estado_pagamento,
	amount_untaxed_signed as valor_sem_iva,
	amount_tax_signed as valor_iva,
	amount_total_signed as valor_total,
	amount_residual_signed as valor_em_divida,
	CONCAT('https://enterprise.bluwalk.com/pt/my/invoices/',id,'?access_token=',access_token) as link
from {{ ref('stg_odoo_enterprise__account_moves') }}
where 
	journal_id = 104 and
	partner_id != 1586
	