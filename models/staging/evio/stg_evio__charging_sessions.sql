with

source as (
    select
        *
    from {{ source('evio', 'charging_sessions') }}
),

transformation as (

    SELECT
    
       _id as id,
       total_power,
       estimated_price,
       final_price,
       battery_charged,
       time_charged,
       co2_saved,
       payment_status,
       auth_type,
       payment_billing_info,
       hw_id,
       ev_id,
       status,
       plug_id,
       id_tag,
       start_date as start_timestamp,
       DATETIME(TIMESTAMP(start_date), 'Europe/Lisbon') as start_date,
       stop_date as stop_timestamp,
       DATETIME(TIMESTAMP(stop_date), 'Europe/Lisbon') as stop_date,
       session_id,
       cdr_id,
       payment_method,
       card_number,
       final_price_excl_vat,
       ev_brand,
       ev_model,
       ev_license_plate,
       load_timestamp

    FROM source

)

SELECT * FROM transformation