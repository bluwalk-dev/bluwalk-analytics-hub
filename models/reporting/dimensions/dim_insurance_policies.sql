SELECT 
    a.id as insurance_policy_id,  -- Unique identifier for the vehicle insurance record
    a.name as insurance_policy_name,
    c.insurance_type_id,
    c.insurance_type,  -- Type of insurance policy
    c.insurance_type_line,  -- Type of insurance policy
    a.auto_vehicle_id insurance_vehicle_id,  -- Identifier for the associated vehicle
    a.partner_id as insurance_contact_id,
    a.insurer_id as insurer_contact_id,  -- Identifier for the insurer contact
    b.contact_full_name as insurer_name,  -- Full name of the insurer
    a.policy_number insurance_policy_number,  -- Policy number of the insurance
    a.start_date insurance_start_date,  -- Start date of the insurance policy
    a.renewal_date insurance_renewal_date,  -- End date of the insurance policy
    a.state insurance_state  -- Current state of the insurance policy
FROM {{ ref('stg_odoo__insurance_policies') }} a  -- Source table: staged fleet vehicle insurances data
LEFT JOIN {{ ref('dim_contacts') }} b ON a.insurer_id = b.contact_id  -- Joining with contacts dimension table for insurer details
LEFT JOIN {{ ref('dim_insurance_types') }} c ON a.policy_type_id = c.insurance_type_id  -- Joining with contacts dimension table for insurer details