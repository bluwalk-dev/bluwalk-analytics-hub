with

source as (
    select
        *
    from {{ source('evio', 'charging_sessions') }}
),

transformation as (

    SELECT
       TO_HEX(MD5(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_ADD(stop_date, INTERVAL 500000 MICROSECOND), SECOND))) || '+00' || CONCAT('EVIO', right(card_number, 6)) || total_power)) as transaction_key,
       _id as transaction_id,
       total_power,
       estimated_price,
       final_price,
       final_price_excl_vat,
       battery_charged,
       round(time_charged*60, 2) as time_charged_min,
       co2_saved,
       payment_status,
       auth_type,
       payment_billing_info,
       hw_id charger_id,
       plug_id charger_plug_id,
       status,
       ev_id,
       id_tag,
       FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_ADD(start_date, INTERVAL 500000 MICROSECOND), SECOND))) || '+00' as start_timestamp,
       DATETIME(TIMESTAMP(start_date), 'Europe/Lisbon') as start_date,
       FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_ADD(stop_date, INTERVAL 500000 MICROSECOND), SECOND))) || '+00' as stop_timestamp,
       DATETIME(TIMESTAMP(stop_date), 'Europe/Lisbon') as stop_date,
       session_id,
       cdr_id,
       payment_method,
       CONCAT('EVIO', right(card_number, 6)) card_name,
       card_number,
       ev_brand as asset_type,
       ev_model as transaction_type,
       ev_license_plate as user_vat,
       load_timestamp

    FROM source
    order by start_date desc
)

SELECT * FROM transformation