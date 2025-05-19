WITH 

connected_vehicle_active_users AS (
    SELECT 
        year_month, 
        nr_active_users as nr_active_users_connected_vehicle
    FROM {{ ref('agg_retention_monthly_retention_segment_users_active') }}
    WHERE retention_segment = 'RDS: Connected Vehicle'
),
connected_vehicle_deal_won AS (
    SELECT year_month, nr_won_deals FROM 
    {{ ref('agg_sales_monthly_pipeline_deal_won') }} 
    WHERE deal_pipeline_id = '155110085'
)

SELECT 
    a.year_month, 
    -- User Activity Metrics
    i.nr_active_customers,
    m.nr_active_users,
    l.nr_activations,
    j.churn_rate,
    h.nr_active_users_connected_vehicle,
    k.nr_won_deals as won_deals_connected_vehicle,

    -- Fleet Metrics
    f.total_rentals as total_fleet_rentals,
    f.driver_rentals as driver_fleet_rentals,
    f.company_rentals as company_fleet_rentals,
    g.fleet_size,

    -- Fuel and Energy Metrics
    b.diesel_potential_users,
    b.electricity_potential_users,
    b.diesel_actual_users,
    b.electricity_actual_users,
    b.diesel_quantity,
    b.electricity_quantity,
    b.diesel_user_deduction,
    b.electricity_user_deduction,

    -- Academy Metrics
    c.total_trainings as academy_total,
    c.onsite_trainings as academy_on_site,

    -- Insurance Metrics
    d.insurance_revenue,
    e.nr_won_deals as insurance_won_deals

FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('rpt_energy_report') }} b ON a.year_month = b.year_month
LEFT JOIN {{ ref('agg_sm_monthly_training') }} c ON c.year_month = a.year_month
LEFT JOIN {{ ref('agg_sm_monthly_insurance_revenue') }} d ON d.year_month = a.year_month
LEFT JOIN {{ ref('agg_sm_monthly_insurance_deal_won') }} e ON e.year_month = a.year_month
LEFT JOIN {{ ref('agg_fleet_rental_monthly') }} f on a.year_month = f.year_month
LEFT JOIN {{ ref('agg_fleet_monthly_size') }} g on a.year_month = g.year_month
LEFT JOIN connected_vehicle_active_users h ON a.year_month = h.year_month
LEFT JOIN {{ ref('agg_wm_monthly_customers_active') }} i ON a.year_month = i.year_month
LEFT JOIN {{ ref('agg_wm_monthly_users_activations')}} l ON a.year_month = l.year_month
LEFT JOIN {{ ref('agg_wm_monthly_users_active')}} m ON a.year_month = m.year_month
LEFT JOIN {{ ref('agg_wm_monthly_users_churn_rate') }} j ON a.year_month = j.year_month
LEFT JOIN connected_vehicle_deal_won k ON a.year_month = k.year_month
WHERE a.start_date <= CURRENT_DATE()
ORDER BY year_month DESC