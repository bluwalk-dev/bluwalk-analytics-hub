WITH 
contract_periods AS (
    SELECT 
        lease_contract_name,
        lease_contract_id,
        customer_id,
        start_date,
        vehicle_id,
        0.16 as extra_mileage_price,
        COALESCE(end_date, CURRENT_DATE()) AS end_date,
        start_kms,
        end_kms
    FROM {{ ref('base_fleet_lease_contracts') }}
    WHERE start_date <= COALESCE(end_date, CURRENT_DATE())
),
vehicle_odometer AS (
    SELECT
      a.year_week,
      a.vehicle_id,
      MAX(b.odometer) AS start_odometer,
      MAX(a.odometer) AS end_odometer
    FROM {{ ref('fct_fleet_vehicle_odometers') }} AS a
    LEFT JOIN {{ ref('fct_fleet_vehicle_odometers') }} AS b
    ON a.vehicle_id = b.vehicle_id AND b.date = DATE_SUB(DATE_TRUNC(a.date, WEEK(MONDAY)), INTERVAL 1 DAY)
    GROUP BY a.year_week, a.vehicle_id
),
-- Subquery to calculate start_kms and end_kms
kms_calculations AS (
    SELECT 
        cp.lease_contract_id,
        cp.lease_contract_name,
        cp.vehicle_id,
        cp.customer_id,
        cp.extra_mileage_price,
        cal.year_week,
        GREATEST(cal.start_date, cp.start_date) AS period_start,
        LEAST(cal.end_date, cp.end_date) AS period_end,
        
        -- Calculate start_kms: Use contract's start_kms or vehicle's odometer
        CASE 
            WHEN GREATEST(cal.start_date, cp.start_date) = cp.start_date THEN cp.start_kms
            ELSE vo.start_odometer
        END AS start_kms,
        
        -- Calculate end_kms: Use contract's end_kms or vehicle's odometer
        CASE 
            WHEN LEAST(cal.end_date, cp.end_date) = cp.end_date THEN cp.end_kms
            ELSE vo.end_odometer
        END AS end_kms,
        
        -- Calculate rental days
        DATE_DIFF(LEAST(cal.end_date, cp.end_date), GREATEST(cal.start_date, cp.start_date), DAY) + 1 AS rental_days
        
    FROM contract_periods cp
    JOIN {{ ref('util_week_intervals') }} cal ON cal.start_date <= cp.end_date AND cal.end_date >= cp.start_date
    LEFT JOIN vehicle_odometer vo ON cp.vehicle_id = vo.vehicle_id AND cal.year_week = vo.year_week
)
SELECT 
    cp.lease_contract_name,
    cp.lease_contract_id,
    lcc.id as conditions_id,
    vehicle_id,
    customer_id,    
    period_start,
    period_end,
    year_week,
    extra_mileage_price,
    
    -- Directly use start_kms and end_kms calculated from the subquery
    start_kms,
    end_kms,
    
    -- Calculate distance as the difference between end_kms and start_kms
    ROUND(end_kms - start_kms,0) AS distance,
    
    rental_days,
    
    -- Calculate included_kms
    ROUND((lcc.rate_mileage_wk_limit / 7) * rental_days) AS included_kms,
    
    -- Calculate extra kms
    GREATEST(ROUND(end_kms - start_kms,0) - ROUND((lcc.rate_mileage_wk_limit / 7) * rental_days), 0) AS extra_km,
    
    -- Calculate extra kms revenue
    ROUND(extra_mileage_price * GREATEST(ROUND(end_kms - start_kms,0) - ROUND((lcc.rate_mileage_wk_limit / 7) * rental_days), 0), 2) extra_km_revenue

    
FROM kms_calculations cp
LEFT JOIN {{ ref('stg_odoo_drivfit__lease_contract_conditions') }} lcc ON 
  cp.lease_contract_id = lcc.lease_contract_id AND 
  cp.period_start >= lcc.effective_date AND 
  cp.period_end <= COALESCE(lcc.termination_date, CURRENT_DATE())
ORDER BY lease_contract_id, period_start