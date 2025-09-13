SELECT
    a.transaction_key,
    a.transaction_id,
    a.local_date_time,
    a.timestamp,
    a.card_name,
    a.station_name,
    a.product,
    a.quantity,
    a.value_station_price,
    a.energy_id,
    b.contact_id,
    c.is_forwarded,
    c.transaction_account_id,
    c.analytic_account_id
FROM {{ ref("base_bp_transactions") }} a
LEFT JOIN {{ ref("dim_energy_cards") }} b ON a.card_name = b.card_name AND a.local_date_time BETWEEN b.delivery_date AND COALESCE(b.receive_date, CURRENT_TIMESTAMP())
LEFT JOIN {{ ref("rpt_finances_service_import_helper") }} c ON b.contact_id = c.contact_id
ORDER BY a.local_date_time DESC