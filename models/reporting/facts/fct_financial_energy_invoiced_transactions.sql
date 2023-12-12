SELECT
    transaction_key,
    energy_id,
    energy_source,
    invoice_nr,
    invoice_date,
    station_name,
    product,
    timestamp,
    card_name,
    quantity,
    value_net,
    value_vat,
    value_total
FROM {{ ref("base_bp_invoice_transactions") }}

UNION ALL

SELECT
    transaction_key,
    energy_id,
    energy_source,
    invoice_nr,
    invoice_date,
    station_name,
    product,
    timestamp,
    card_name,
    quantity,
    value_net,
    value_vat,
    value_total
FROM {{ ref("base_prio_invoice_transactions") }}

ORDER BY timestamp DESC