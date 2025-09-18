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
    a.total_net_value,
    a.total_value,
    b.energy_id
FROM bluwalk-analytics-hub.staging.stg_prio_electric_sessions a
LEFT JOIN {{ ref("base_service_orders_electricity") }} b ON a.transaction_key = b.transaction_key
ORDER BY start_timestamp DESC