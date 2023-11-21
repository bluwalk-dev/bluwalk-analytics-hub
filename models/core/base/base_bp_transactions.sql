WITH unique_transactions AS (
    SELECT * FROM (
        SELECT
            *, ROW_NUMBER() OVER (
                PARTITION BY transaction_key 
                ORDER BY extraction_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_bp__transactions") }}
    )
    WHERE __row_number = 1
)

SELECT
    a.transaction_key,
    a.transaction_id,
    a.transaction_status,
    a.local_date_time,
    a.timestamp,
    a.card_name,
    a.station_name,
    a.nr_items,
    a.product,
    a.quantity,
    a.value_station_price,
    a.transaction_type,
    a.transaction_outcome,
    a.confirmation_status,
    a.issuer_value,
    a.supplier_value,
    a.card_profile,
    b.energy_id
FROM unique_transactions a
LEFT JOIN {{ ref("base_service_orders_fuel") }} b ON a.transaction_key = b.transaction_key
WHERE a.transaction_status LIKE 'Aceite'
ORDER BY a.local_date_time DESC