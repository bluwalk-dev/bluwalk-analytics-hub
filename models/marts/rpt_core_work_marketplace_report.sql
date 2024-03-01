WITH 

user_income as (
    SELECT 
        b.year_month,
        a.product_category as partner_category,
        sum(a.amount) as user_income
    FROM {{ ref('fct_financial_user_transactions') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
    WHERE 
        b.year >= 2021 AND 
        product_group = 'Income' AND 
        a.amount > 0 AND
        a.product_category IN ('Ridesharing', 'Shopping', 'Courier', 'Food Delivery')
    GROUP BY year_month, partner_category
),

user_net_income as (
    SELECT 
        b.year_month,
        a.retention_segment,
        sum(a.user_net_income) as user_net_income
    FROM {{ ref('agg_retention_segments_daily_activity') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
    GROUP BY year_month, retention_segment
),

maw as (
    SELECT year_month, partner_category, nr_active_users 
    FROM {{ ref('agg_cm_monthly_partner_category_users_active') }}
    WHERE partner_category IN ('TVDE', 'Shopping', 'Courier', 'Food Delivery')
),

rideshare_PV_MAW as (
    SELECT 
        year_month, 
        nr_active_users rs_pv_maw 
    FROM {{ ref('agg_retention_monthly_retention_segment_users_active') }}
    WHERE retention_segment = 'RDS: Connected Vehicle'
),

rideshare_EV_MAW as (
    SELECT year_month, count(*) rs_ev_maw
    FROM (
        SELECT DISTINCT c.year_month, a.contact_id
        FROM {{ ref("fct_user_rideshare_trips") }} a
        LEFT JOIN {{ ref("dim_vehicle_contracts") }} b on a.vehicle_contract_id = b.vehicle_contract_id
        LEFT JOIN {{ ref("util_calendar") }} c ON CAST(a.dropoff_local_time AS DATE) = c.date
        where b.vehicle_fuel_type = 'electric'
    )
    GROUP BY year_month
)

SELECT
    a.year_month,

    a.nr_activations as total_acquired,
    a.nr_churns as total_churned,
    a.churn_rate as overall_churn_rate,

    c1.nr_active_users as rs_maw, 
    c2.nr_active_users as gc_maw,
    c3.nr_active_users as pc_maw,
    c4.nr_active_users as fd_maw,

    b1.user_income as rs_income,
    b2.user_income as gc_income,
    b3.user_income as pc_income,
    b4.user_income as fd_income,

    d.rs_pv_maw, 
    e.rs_ev_maw,

    f.nr_activations as rs_rv_acquired,
    f.nr_churns as rs_rv_churned,
    g.user_net_income as rs_rv_total_net_income,

    h.nr_activations as rs_pv_activations,
    h.nr_churns as rs_pv_churn,
    i.user_net_income as rs_pv_total_income
FROM {{ ref('dm_core_monthly_marketplace_users') }} a

LEFT JOIN user_income b1 ON a.year_month = b1.year_month AND b1.partner_category = 'Ridesharing'
LEFT JOIN user_income b2 ON a.year_month = b2.year_month AND b2.partner_category = 'Shopping'
LEFT JOIN user_income b3 ON a.year_month = b3.year_month AND b3.partner_category = 'Courier'
LEFT JOIN user_income b4 ON a.year_month = b4.year_month AND b4.partner_category = 'Food Delivery'

LEFT JOIN maw c1 ON a.year_month = c1.year_month AND c1.partner_category = 'TVDE'
LEFT JOIN maw c2 ON a.year_month = c2.year_month AND c2.partner_category = 'Shopping'
LEFT JOIN maw c3 ON a.year_month = c3.year_month AND c3.partner_category = 'Courier'
LEFT JOIN maw c4 ON a.year_month = c4.year_month AND c4.partner_category = 'Food Delivery'

LEFT JOIN rideshare_PV_MAW d ON a.year_month = d.year_month
LEFT JOIN rideshare_EV_MAW e ON a.year_month = e.year_month

LEFT JOIN {{ ref('dm_core_monthly_partner_users') }} f ON a.year_month = f.year_month AND f.partner_key = '79bb6516ccd466bcb106aa89b13ad905'
LEFT JOIN user_net_income g ON a.year_month = g.year_month AND g.retention_segment = 'RDS: Vehicle Rental'

LEFT JOIN {{ ref('dm_core_monthly_partner_users') }} h ON a.year_month = h.year_month AND h.partner_key = '577c5ad4204a5f35d4b9c45ab285069d'
LEFT JOIN user_net_income iy ON a.year_month = i.year_month AND i.retention_segment = 'RDS: Connected Vehicle'

WHERE a.partner_marketplace = 'Work'
ORDER BY a.year_month DESC