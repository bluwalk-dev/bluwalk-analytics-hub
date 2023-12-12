WITH unique_transactions AS (
    SELECT * FROM (
        SELECT
            *, ROW_NUMBER() OVER (
                PARTITION BY transaction_key 
                ORDER BY extraction_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_bp__invoice_transactions") }}
    )
    WHERE __row_number = 1
)

SELECT
    a.transaction_key,
    c.transaction_id,
    b.energy_id,
    b.energy_source,
    a.invoice,
    a.invoice_date,
    a.nr_of_transactions,
    a.invoice_nr,
    a.currency,
    a.transaction_nr,
    a.station_name,
    a.product,
    a.timestamp,
    a.card_name,
    a.quantity,
    a.value_net,
    a.value_vat,
    a.value_total,
    a.vat_rate
FROM unique_transactions a
LEFT JOIN {{ ref("base_service_orders_fuel") }} b ON a.transaction_key = b.transaction_key
LEFT JOIN {{ ref("base_bp_transactions") }} c ON a.transaction_key = c.transaction_key
ORDER BY a.local_date_time DESC