select
    a.id,
    a.date,
    a.name,
    a.amount,
    a.partner_id contact_id,
    a.product_id,
    a.categ_id product_category_id,
    a.account_id,
    c.account_type,
    c.account_name,
    c.account_owner_contact_id,
    a.move_id,
    b.move_name,
    a.ref reference,
    CASE
        WHEN a.fuel_id IS NOT NULL THEN 'Fuel'
        WHEN a.trips_id IS NOT NULL THEN 'Job'
        WHEN a.rental_contract_id IS NOT NULL THEN 'Vehicle Rental'
        WHEN a.tolls_id IS NOT NULL THEN 'Toll'
        WHEN a.insurance_claim_id IS NOT NULL THEN 'Insurance Claim'
        WHEN a.vehicle_damage_id IS NOT NULL THEN 'Vehicle Damage'
        WHEN a.rental_vehicle_id IS NOT NULL THEN  'Vehicle Rental'
        ELSE NULL
    END order_type,
    CASE
        WHEN a.fuel_id IS NOT NULL THEN a.fuel_id
        WHEN a.trips_id IS NOT NULL THEN a.trips_id
        WHEN a.rental_contract_id IS NOT NULL THEN a.rental_contract_id
        WHEN a.tolls_id IS NOT NULL THEN a.tolls_id
        WHEN a.insurance_claim_id IS NOT NULL THEN a.insurance_claim_id
        WHEN a.vehicle_damage_id IS NOT NULL THEN a.vehicle_damage_id
        WHEN a.rental_vehicle_id IS NOT NULL THEN a.rental_vehicle_id
        ELSE NULL
    END order_id,
    a.payment_cycle statement
from {{ ref('stg_odoo__account_analytic_lines') }} a
left join {{ ref('fct_accounting_move_lines') }} b ON a.move_id = b.id
left join {{ ref('dim_accounting_analytic_accounts') }} c ON a.account_id = c.account_id
ORDER BY date DESC