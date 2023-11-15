/* 
Insurance Policy connection missing
User ID connection missing
*/

SELECT 
	a.id vehicle_insurance_id,
    a.vehicle_id,
    a.insurance_type,
    a.insurer_id insurer_contact_id,
    b.full_name insurer_name,
    a.policy_id,
    a.policy_number,
    a.start_date,
    a.end_date,
    a.state,
    ROUND(a.annual_cost, 2) annual_cost,
    ROUND(a.annual_cost / 12, 2) monthly_cost,
    ROUND(a.annual_cost / 365 * date_diff(end_date, start_date, DAY),2) insurance_contract_cost,
FROM {{ ref('stg_odoo__fleet_vehicle_insurances') }} a
LEFT JOIN {{ ref('dim_contacts') }} b ON a.insurer_id = b.contact_id