select
    a.id as vehicle_model_id,
    CONCAT(b.name, ' ', a.name) AS vehicle_name,
    b.id as vehicle_brand_id,
    b.name as vehicle_brand,
    a.name as vehicle_model
FROM {{ ref('stg_odoo_drivfit__fleet_vehicle_models') }} a
LEFT JOIN {{ ref('stg_odoo_drivfit__fleet_vehicle_model_brands') }} b ON a.brand_id = b.id
