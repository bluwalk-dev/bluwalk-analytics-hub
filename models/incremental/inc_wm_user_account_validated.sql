-- CTE to gather information about Uber drivers who have completed their onboarding
WITH uberDriverStatus AS (
    SELECT
        driver_uuid,  -- Unique identifier for the driver
        DATETIME(MIN(extraction_ts), 'Europe/Lisbon') as account_validated_date  -- The earliest timestamp converted to a date indicating when the account was validated
    FROM {{ ref("stg_uber__driver_status") }}  -- Reference to the staging model for uber driver status
    WHERE onboarding_status = 'ONBOARDING_STATUS_ACTIVE'  -- Filter for drivers who are active
    GROUP BY driver_uuid  -- Grouping by driver to ensure unique driver records
)

-- Main query to select deals and associated Uber driver information
SELECT
  a.user_id,  -- The user ID from the base HubSpot deals model
  'Uber' as partner_name,  -- Hardcoded value of 'Uber' to indicate the partner name
  c.account_validated_date  -- The account validation date from the CTE
FROM {{ ref("fct_deals") }} a  -- Reference to the base deals model
LEFT JOIN {{ ref("dim_partners_accounts") }} b ON a.contact_id = b.contact_id  -- Joining on contacts to get partner accounts
LEFT JOIN uberDriverStatus c ON b.partner_account_uuid = c.partner_account_uuid  -- Joining with the CTE to get the driver status information
WHERE
  a.deal_pipeline_stage_id IN ('294193864', '310796490') AND  -- Filtering for specific deal stages
  a.is_closed = FALSE AND  -- Excluding closed deals
  c.partner_account_uuid IS NOT NULL  -- Ensuring that there is a matched driver record