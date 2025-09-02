SELECT
    a.id as transaction_line_id,
    a.name as transaction_line_name,
    b.user_id,
    d.contact_id,
    a.account_id,
    b.transaction_account_name as account_name,
    b.transaction_account_type as account_type,
    a.transaction_name,
    a.transaction_hash,
    a.date,
    a.category_id,
    c.name as category_name,
    c.group_id,
    e.name as group_name,
    a.payment_cycle,
    financial_document_id,
    a.amount,
    a.amount_residual,
    a.amount_balance,
    a.description,
    a.trips_id,
    a.fuel_id,
    a.rental_vehicle_id,
    a.misc_id,
    CASE
        WHEN a.fuel_id IS NOT NULL THEN 'Fuel'
        WHEN a.trips_id IS NOT NULL THEN 'Work'
        WHEN a.rental_vehicle_id IS NOT NULL THEN  'Vehicle Rental'
        ELSE NULL
    END order_type,
    CASE
        WHEN a.fuel_id IS NOT NULL THEN a.fuel_id
        WHEN a.trips_id IS NOT NULL THEN a.trips_id
        WHEN a.rental_vehicle_id IS NOT NULL THEN a.rental_vehicle_id
        ELSE NULL
    END order_id
FROM bluwalk-analytics-hub.staging.stg_odoo_bw_transaction_lines a
LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_bw_transaction_accounts b ON a.account_id = b.transaction_account_id
LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_bw_transaction_categories c ON a.category_id = c.id
LEFT JOIN bluwalk-analytics-hub.core.core_users d ON b.user_id = d.user_id
LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_bw_transaction_groups e ON c.group_id = e.id