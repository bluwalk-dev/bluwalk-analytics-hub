SELECT
    a.id as booking_id,
    a.name as booking_name,
    a.state as booking_state,
    a.booking_type,
    a.driver_id,
    e.contact_name as driver_name,
    e.contact_vat as driver_vat,
    a.client_id as customer_id,
    g.contact_name customer_name,
    b.vehicle_id,
    b.vehicle_license_plate,
    b.vehicle_deal_id,
    b.vehicle_deal_name, -- Atenção que talvez tenhamos o mesmo carro em mais do que um deal
    b.vehicle_name,
    start_datetime as booking_pickup_datetime,
    a.start_station_id as booking_pickup_station_id,
    c.station_name as booking_pickup_station_name,
    
    a.rate_base_id,
    h.weekly_value as booking_rate_weekly_price,
    h.public_name as booking_rate_name,
    h.contract_type as booking_contract_type,
    a.rate_mileage_id,
    (a.mileage_limit * 7) as booking_mileage_limit,
    a.mileage_excess,
    a.rate_protection_id,

    a.create_date
    
FROM {{ ref('stg_odoo_drivfit__bookings') }} a
LEFT JOIN {{ ref('dim_fleet_vehicles') }} b ON a.vehicle_id = b.vehicle_id
LEFT JOIN {{ ref('dim_fleet_stations') }} c ON a.start_station_id = c.station_id
LEFT JOIN {{ ref('int_odoo_drivfit_contacts') }} e ON a.driver_id = e.contact_id
LEFT JOIN {{ ref('int_odoo_drivfit_contacts') }} g ON a.client_id = g.contact_id
LEFT JOIN {{ ref('stg_odoo_drivfit__rate_bases') }} h ON a.rate_base_id = h.id
LEFT JOIN {{ ref('stg_odoo_drivfit__vehicle_categories') }} i ON h.vehicle_category_id = i.id  -- Joining with vehicle categories