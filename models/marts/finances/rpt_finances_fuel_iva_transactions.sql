SELECT 
    f.card_name,
    f.quantity, 
    fd.amount AS ba_debit, 
    rp.user_vat,
    rp.user_name AS driver_name,
    c.year_week AS week, 
    rc.vehicle_plate as license_plate,
    rc.vehicle_contract_name AS contract_nr,
    CASE 
        WHEN vehicle_contract_type = 'car_rental' THEN 'Drivfit'
        ELSE 'Contrato de Comodato'
    END supplier,
    f.energy_source AS fuel_source,
FROM {{ ref('base_service_orders_fuel') }} f
LEFT JOIN (
    SELECT order_id as id, -1 * sum(amount) as amount 
    FROM {{ ref('fct_financial_user_transactions') }} 
    WHERE 
        order_type = 'Fuel' AND 
        product_id IN (33, 35, 37) -- only diesel and gasoline
    GROUP BY order_id) fd ON fd.id = f.energy_id
LEFT JOIN {{ ref('util_calendar') }} c ON CAST(f.start_date AS DATE) = c.date
LEFT JOIN {{ ref('dim_users') }} rp ON f.contact_id = rp.contact_id
LEFT JOIN {{ ref('dim_vehicle_contracts') }} rc ON f.contact_id = rc.contact_id 
WHERE 
    f.start_date >= rc.start_date AND 
    f.start_date <= IFNULL(rc.end_date, CAST('2050-12-31' AS DATE)) AND
    (f.energy_source = 'diesel' OR f.energy_source = 'gasoline')