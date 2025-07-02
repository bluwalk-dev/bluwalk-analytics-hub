SELECT * 
FROM (
select 
    x.vat,
    y.name,
    y.email,
    ifnull(y.mobile, y.phone) phone,
    sum(balance) as current_deposits
from (
    -- Odoo Enterprise
    SELECT
        a.date,
        c.vat,
        c.name,
        -a.balance as balance
    FROM {{ ref('stg_odoo_enterprise__account_move_lines') }} a
    LEFT JOIN {{ ref('stg_odoo_enterprise__account_accounts') }} b on a.account_id = b.id
    LEFT JOIN {{ ref('stg_odoo_enterprise__res_partners') }} c on a.partner_id = c.id
    where journal_id = 132 and b.code like '278%'

    UNION ALL

    -- Odoo Community
    select
        a.date,
        b.vat,
        b.name,
        a.amount_untaxed_signed
    from {{ ref('stg_odoo_drivfit__account_moves') }} a
    left join {{ ref('stg_odoo_drivfit__res_partners') }} b on a.partner_id = b.id
    where a.state = "posted" and a.type in ("out_deposit","out_refund_deposit") and a.date < '2025-01-01'
) x 
LEFT JOIN {{ ref('stg_odoo_drivfit__res_partners') }} y ON x.vat = y.vat
GROUP BY vat, name, email, phone
) WHERE current_deposits > 0