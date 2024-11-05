SELECT
    a.transaction_key,
    a.transaction_id,
    a.start_timestamp,
    a.stop_timestamp,
    a.card_name,
    a.charger_id,
    a.total_power,
    a.final_price,
    a.energy_id,
    b.contact_id,
    c.is_forwarded,
    c.transaction_account_id,
    c.analytic_account_id
FROM {{ ref("base_evio_charging_sessions") }} a
LEFT JOIN {{ ref("dim_energy_cards") }} b ON a.card_name = b.card_name AND CAST(a.start_timestamp AS TIMESTAMP) BETWEEN CAST(b.delivery_date AS TIMESTAMP) AND CAST(COALESCE(b.receive_date, CURRENT_DATE()) AS TIMESTAMP)
LEFT JOIN {{ ref("rpt_finances_service_import_helper") }} c ON b.contact_id = c.contact_id
WHERE 
    energy_id is null AND
    start_date < DATETIME_SUB(DATETIME(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 1 DAY)), INTERVAL 1 SECOND)