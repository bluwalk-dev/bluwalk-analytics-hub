SELECT 
    id as insurance_type_id,
    name as insurance_type,
    policy_class as insurance_type_line
FROM {{ ref('stg_odoo__insurance_policy_types') }}