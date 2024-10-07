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
WHERE 
    energy_id is null AND
    start_date < DATETIME_SUB(DATETIME(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 1 DAY)), INTERVAL 1 SECOND)