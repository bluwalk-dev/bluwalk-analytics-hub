WITH sessions AS (
    SELECT * FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY load_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_prio__ev_transactions") }} 
        )
    WHERE __row_number = 1
)

SELECT
    a.transaction_key,
    a.transaction_id,
    a.start_timestamp,
    a.start_date,
    a.stop_timestamp,
    a.stop_date,
    a.card_name,
    a.charger_id,
    a.quantity,
    a.total_value,
    b.energy_id
FROM sessions a
LEFT JOIN {{ ref("base_service_orders_electricity") }} b ON a.transaction_key = b.transaction_key
ORDER BY start_timestamp DESC