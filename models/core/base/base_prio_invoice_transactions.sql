SELECT
    a.transaction_key,
    b.energy_id,
    b.energy_source,
    a.invoice_nr,
    a.invoice_date,
    a.station_name,
    a.product,
    a.timestamp,
    a.card_name,
    a.quantity,
    a.value_net,
    a.value_vat,
    a.value_total
FROM {{ ref("stg_prio__ff_invoice_transactions") }} a
LEFT JOIN {{ ref("base_service_orders_fuel") }} b ON a.transaction_key = b.transaction_key
ORDER BY a.local_date_time DESC