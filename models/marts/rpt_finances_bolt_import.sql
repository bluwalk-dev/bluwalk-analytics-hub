WITH user_accounts AS (
    SELECT *
    FROM {{ ref('dim_accounting_analytic_accounts') }}
    WHERE analytic_account_type = 'User' AND analytic_account_state = 'active'
)

SELECT
    b.contact_id,
    b.sales_partner_id,
    3738 sales_contact_id,
    a.partner_account_uuid,
    location_name sales_account_city,
    'Trips' sales_segment,
    a.date period_start,
    a.date period_end,
    IFNULL(f.nr_trips, 0) nr_trips,
    gross_revenue gross_sales,
    ROUND(gross_revenue/(1+c.sales_tax_rate/100),2) net_sales,
    gross_revenue - ROUND(gross_revenue/(1+c.sales_tax_rate/100),2) sales_taxes,
    c.sales_tax_rate,
    ROUND(gross_revenue - net_earnings, 2) gross_partner_fee,
    ROUND(gross_revenue - net_earnings, 2) net_partner_fee,
    0 partner_fee_taxes,
    0 partner_fee_rate,
    net_earnings partner_payment,
    year_week payment_cycle,

    - (gross_revenue - ROUND(gross_revenue/(1+c.sales_tax_rate/100),2)) amount_vat,
    CASE WHEN c.sales_tax_rate = 6 THEN 31 WHEN c.sales_tax_rate = 5 THEN 30 END product_id_vat,
    'IVA Bolt Trips' description_vat,
    '.' external_notes_vat,
    e.analytic_account_id analytic_account_id_vat,
    b.contact_id contact_id_vat,
    
    net_earnings amount_payout,
    26 product_id_payout,
    'Pagamento Bolt Trips' description_payout,
    '.' external_notes_payout,
    e.analytic_account_id analytic_account_id_payout,
    b.contact_id contact_id_payout,

    - ROUND(gross_revenue/(1+c.sales_tax_rate/100),2) amount_revenue,
    52 product_id_revenue,
    'Bolt Trips Revenue' description_revenue,
    '.' external_notes_revenue,
    28859 analytic_account_id_revenue,
    3738 contact_id_revenue,

    ROUND(gross_revenue - net_earnings, 2) amount_intfee,
    52 product_id_intfee,
    'Bolt Trips Intermediation Fees' description_intfee,
    '.' external_notes_intfee,
    9539 analytic_account_id_intfee,
    3738 contact_id_intfee,

    - ROUND(net_earnings * (IFNULL(service_fee, 5) / 100), 2) amount_sfee_user,
    136 product_id_sfee_user,
    'Fee Serviço Bluwalk' description_sfee_user,
    '.' external_notes_sfee_user,
    e.analytic_account_id analytic_account_id_sfee_user,
    b.contact_id contact_id_sfee_user,

    ROUND(net_earnings * (IFNULL(service_fee, 5) / 100), 2) amount_sfee_gp,
    CASE WHEN service_fee = 15 THEN 156 ELSE 137 END product_id_sfee_gp,
    'Bluwalk Service Fee' description_sfee_gp,
    '.' external_notes_sfee_gp,
    21979 analytic_account_id_sfee_gp,
    3738 contact_id_sfee_gp

FROM {{ ref('base_bolt_earnings') }} a
LEFT JOIN {{ ref('dim_partners_accounts') }} b ON a.partner_account_uuid = b.partner_account_uuid
LEFT JOIN {{ ref('dim_partners_logins') }} c ON a.bolt_company_id = c.login_id
LEFT JOIN {{ ref('dim_locations') }} d ON c.location_id = d.location_id
LEFT JOIN user_accounts e ON b.contact_id = e.analytic_account_owner_contact_id
LEFT JOIN {{ ref('base_bolt_performance') }} f ON a.date = f.date AND a.partner_account_uuid = f.partner_account_uuid
LEFT JOIN {{ ref('util_calendar') }} g ON a.date = g.date
LEFT JOIN {{ ref('int_user_service_fee_per_day') }} h ON b.user_id = h.user_id AND a.date = h.date
ORDER BY a.date DESC, sales_account_city, partner_account_uuid