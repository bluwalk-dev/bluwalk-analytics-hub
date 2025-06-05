WITH insurance_claims AS (
  SELECT
    id AS claim_id,
    CAST(claim_date AS DATE) AS claim_date,
    name AS claim_name,
    claim_process_nr,
    responsability_final,
    repair_cost,
    stoppage_location
  FROM {{ ref('stg_odoo_drivfit__fleet_vehicle_insurance_claims') }}
  WHERE 
    state = 'closed' AND
    claim_date >= '2025-03-01'
),

repair_counts AS (
  SELECT
    insurance_claim_id,
    COUNT(*) AS nr_repairs
  FROM {{ ref('stg_odoo_drivfit__insurance_claim_repair_rel') }}
  GROUP BY insurance_claim_id
),

contact_info AS (
  SELECT
    child.contact_id,
    child.contact_name AS workshop_name,
    COALESCE(parent.contact_name, child.contact_name) AS supplier_name,
    COALESCE(parent.contact_vat, child.contact_vat) AS supplier_vat
  FROM {{ ref('int_odoo_drivfit_contacts') }} child
  LEFT JOIN {{ ref('int_odoo_drivfit_contacts') }} parent ON child.contact_parent_id = parent.contact_id
)

SELECT
  ic.claim_date,
  ci.workshop_name,
  ci.supplier_name,
  ci.supplier_vat,
  ic.claim_name,
  ic.claim_process_nr,
  ic.responsability_final,
  ic.repair_cost,
  round((0.1 * ic.repair_cost),2) as commission,
  FALSE as is_invoiced
FROM insurance_claims ic
JOIN repair_counts rc ON ic.claim_id = rc.insurance_claim_id
LEFT JOIN contact_info ci ON ic.stoppage_location = ci.contact_id