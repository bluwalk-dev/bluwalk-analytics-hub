/*
  vehicle_insurance_enriched model
  This model enriches the fleet vehicle insurance data from the stg_odoo__fleet_vehicle_insurances source
  by joining it with the dim_contacts dimension table. It provides detailed information about
  vehicle insurance policies including insurer details and cost calculations.

  Note: 
  - Insurance Policy connection is currently missing and needs to be established.
  - User ID connection to link the insurance with a specific user is also missing.

  Source Tables:
  - stg_odoo__fleet_vehicle_insurances: Contains basic insurance information for vehicles.
  - dim_contacts: Contains contact details, used here for insurer information.
*/

SELECT 
    a.id as vehicle_insurance_id,  -- Unique identifier for the vehicle insurance record
    a.vehicle_id,  -- Identifier for the associated vehicle
    a.insurance_type,  -- Type of insurance policy
    a.insurer_id as insurer_contact_id,  -- Identifier for the insurer contact
    b.contact_full_name as insurer_name,  -- Full name of the insurer

    -- Policy details
    a.policy_id,  -- Identifier for the insurance policy
    a.policy_number,  -- Policy number of the insurance
    a.start_date,  -- Start date of the insurance policy
    a.end_date,  -- End date of the insurance policy
    a.state,  -- Current state of the insurance policy

    -- Cost calculations
    ROUND(a.annual_cost, 2) as annual_cost,  -- Annual cost of the insurance, rounded to 2 decimal places
    ROUND(a.annual_cost / 12, 2) as monthly_cost,  -- Monthly cost of the insurance, derived from annual cost
    ROUND(a.annual_cost / 365 * date_diff(end_date, start_date, DAY), 2) as insurance_contract_cost  -- Total cost of the insurance contract for its duration

FROM {{ ref('stg_odoo__fleet_vehicle_insurances') }} a  -- Source table: staged fleet vehicle insurances data
LEFT JOIN {{ ref('dim_contacts') }} b ON a.insurer_id = b.contact_id  -- Joining with contacts dimension table for insurer details