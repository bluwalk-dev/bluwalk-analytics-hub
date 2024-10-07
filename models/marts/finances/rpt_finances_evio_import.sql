SELECT
    transaction_key,
    transaction_id,
    start_timestamp,
    stop_timestamp,
    card_name,
    charger_id,
    total_power,
    final_price,
    energy_id
FROM {{ ref("base_evio_charging_sessions") }}
WHERE energy_id is null