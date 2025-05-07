select
    id as vehicle_deal_id,
    name as vehicle_deal_name,
    quantity as vehicle_deal_quantity,
    brand_id as vehicle_deal_brand,
    model_id as vehicle_deal_model
from {{ ref('stg_odoo_drivfit__fleet_vehicle_deals') }}