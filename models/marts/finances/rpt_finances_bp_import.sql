SELECT
    a.*,
    b.contact_id,
    c.is_forwarded,
    c.transaction_account_id,
    c.analytic_account_id
FROM {{ ref("base_bp_transactions") }} a
LEFT JOIN {{ ref("dim_energy_cards") }} b ON a.card_name = b.card_name AND a.local_date_time BETWEEN b.delivery_date AND COALESCE(b.receive_date, CURRENT_DATE())
LEFT JOIN {{ ref("rpt_finances_service_import_helper") }} c ON b.contact_id = c.contact_id
ORDER BY a.local_date_time DESC