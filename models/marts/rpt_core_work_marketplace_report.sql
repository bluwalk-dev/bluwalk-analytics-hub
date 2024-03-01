WITH user_income as (
    SELECT 
        b.year_month,
        a.product_category as stream_type,
        sum(a.amount) as user_income
    FROM {{ ref('fct_financial_user_transactions') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
    WHERE 
        b.year >= 2021 AND 
        product_group = 'Income' AND 
        a.amount > 0 AND
        a.product_category IN ('Ridesharing', 'Shopping', 'Courier', 'Food Delivery')
    GROUP BY year_month, stream_type
),

wni_retention_segment as (
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

rs_maw_personal_vehicle as (
    SELECT 
        year_month, 
        nr_active_users rs_maw_personal_vehicle 
    FROM {{ ref('agg_retention_monthly_retention_segment_users_active') }}
    WHERE retention_segment = 'RDS: Connected Vehicle'
),

rs_electric_vehicle as (
    SELECT year_month, count(*) rs_electric_vehicle
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
    a.nr_activations as all_acquired_workers,
    a.nr_churns as all_churned_workers,
    a.churn_rate as all_churn_rate,
    maw1.nr_active_users as rs_maw, 
    maw2.nr_active_users as gc_maw,
    maw3.nr_active_users as pc_maw,
    maw4.nr_active_users as fd_maw,
    inc1.user_income as rs_total_income,
    inc2.user_income as gc_total_income,
    inc3.user_income as pc_total_income,
    inc4.user_income as fd_total_income,
    pv.rs_maw_personal_vehicle, 
    rs2.rs_electric_vehicle,
    x.nr_activations as rs_rental_vehicle_activations,
    x.nr_churns as rs_rental_vehicle_churn,
    i.user_net_income as rs_rv_total_income,
    z.nr_activations as rs_personal_vehicle_activations,
    z.nr_churns as rs_personal_vehicle_churn,
    y.user_net_income as rs_pv_total_income
FROM {{ ref('dm_core_monthly_marketplace_users') }} a 
LEFT JOIN user_income inc1 ON a.year_month = inc1.year_month AND inc1.stream_type = 'Ridesharing'
LEFT JOIN user_income inc2 ON a.year_month = inc2.year_month AND inc2.stream_type = 'Shopping'
LEFT JOIN user_income inc3 ON a.year_month = inc3.year_month AND inc3.stream_type = 'Courier'
LEFT JOIN user_income inc4 ON a.year_month = inc4.year_month AND inc4.stream_type = 'Food Delivery'
LEFT JOIN maw maw1 ON a.year_month = maw1.year_month AND maw1.partner_category = 'TVDE'
LEFT JOIN maw maw2 ON a.year_month = maw2.year_month AND maw2.partner_category = 'Shopping'
LEFT JOIN maw maw3 ON a.year_month = maw3.year_month AND maw3.partner_category = 'Courier'
LEFT JOIN maw maw4 ON a.year_month = maw4.year_month AND maw4.partner_category = 'Food Delivery'
LEFT JOIN rs_maw_personal_vehicle pv ON a.year_month = pv.year_month
LEFT JOIN rs_electric_vehicle rs2 ON a.year_month = rs2.year_month
LEFT JOIN {{ ref('dm_core_monthly_partner_users') }} x ON a.year_month = x.year_month AND x.partner_key = '79bb6516ccd466bcb106aa89b13ad905'
LEFT JOIN wni_retention_segment y ON a.year_month = y.year_month AND y.retention_segment = 'RDS: Connected Vehicle'
LEFT JOIN {{ ref('dm_core_monthly_partner_users') }} z ON a.year_month = z.year_month AND z.partner_key = '577c5ad4204a5f35d4b9c45ab285069d'
LEFT JOIN wni_retention_segment i ON a.year_month = i.year_month AND i.retention_segment = 'RDS: Vehicle Rental'
WHERE a.partner_marketplace = 'Work'
ORDER BY a.year_month DESC