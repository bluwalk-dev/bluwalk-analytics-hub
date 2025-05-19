WITH 

connected_vehicle_active_users AS (
    SELECT 
        year_month, 
        nr_active_users as nr_active_users_connected_vehicle
    FROM {{ ref('agg_retention_monthly_retention_segment_users_active') }}
    WHERE retention_segment = 'RDS: Connected Vehicle'
),

academy AS (
    SELECT c.year_month, so.date_order ,sol.*, STRPOS(sol.name,'ONLINE') online 
    FROM {{ ref('stg_odoo__sale_order_lines') }} sol
    LEFT JOIN {{ ref('stg_odoo__sale_orders') }} so ON sol.order_id = so.id
    LEFT JOIN {{ ref('util_calendar') }} c on cast(so.date_order as date) = c.date
    WHERE sol.event_id is not null and price_total = 50 and so.state = 'sale'
),

insurance AS (
    select
        year_month,
        sum(revenue)   as insurance_revenue,
        sum(won_deals) as insurance_won_deals
        from (
            select
                b.year_month,
                sum(a.amount)   as revenue,
                0                as won_deals
            from {{ ref('fct_insurance_policy_payments') }} a
            join {{ ref('util_calendar') }}            b
                on a.start_date = b.date
            group by b.year_month

            union all

            select
                b.year_month,
                0                as revenue,
                count(*)         as won_deals
            from {{ ref('fct_deals') }}   a
            join {{ ref('util_calendar') }} b
                on a.close_date = b.date
            where a.deal_pipeline_id = 'default'
                and a.is_closed       = true
                and a.is_closed_won   = true
            group by b.year_month
        ) t
        group by year_month
        order by year_month
)

SELECT 
    a.year_month, 
    -- User Activity Metrics
    i.nr_active_customers,
    j.nr_active_users,
    j.nr_activations,
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
    c.academy_total,
    d.academy_on_site,

    -- Insurance Metrics
    e.insurance_revenue,
    e.insurance_won_deals

FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('rpt_energy_report') }} b ON a.year_month = b.year_month
LEFT JOIN (SELECT year_month, count(*) academy_total FROM academy GROUP BY year_month) c ON c.year_month = a.year_month
LEFT JOIN (SELECT year_month, count(*) academy_on_site FROM academy WHERE online = 0 GROUP BY year_month) d ON d.year_month = a.year_month
LEFT JOIN insurance e ON e.year_month = a.year_month
LEFT JOIN {{ ref('agg_fleet_rental_monthly') }} f on a.year_month = f.year_month
LEFT JOIN {{ ref('agg_fleet_monthly_size') }} g on a.year_month = g.year_month
LEFT JOIN connected_vehicle_active_users h ON a.year_month = h.year_month
LEFT JOIN {{ ref('agg_cm_monthly_users_active') }} i ON a.year_month = i.year_month
LEFT JOIN (
    SELECT * FROM
    {{ ref('dm_core_monthly_marketplace_users') }} 
    WHERE partner_marketplace = 'Work') j ON a.year_month = j.year_month
LEFT JOIN (
    SELECT year_month, nr_won_deals FROM 
    {{ ref('agg_sales_monthly_pipeline_deal_won') }} 
    WHERE deal_pipeline_id = '155110085') k ON a.year_month = k.year_month
WHERE a.start_date <= CURRENT_DATE()
ORDER BY year_month DESC