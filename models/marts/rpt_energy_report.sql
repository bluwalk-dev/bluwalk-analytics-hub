WITH 

energy_consumption AS (
    SELECT 
        b.year_month, 
        a.energy_source, 
        SUM(ROUND(a.quantity,2)) quantity, 
        SUM(ROUND(a.cost,2) + ROUND(a.margin,2)) user_deduction
    FROM {{ ref('fct_service_orders_energy') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.start_date as DATE) = b.date
    GROUP BY b.year_month, a.energy_source
),

potential_users AS (
    SELECT DISTINCT year_month, contact_id, vehicle_fuel_type
    FROM {{ ref('agg_sm_daily_vehicle_activity') }} a
    WHERE contact_id IS NOT NULL
),

actual_users AS (
    SELECT DISTINCT year_month, contact_id, energy_source
    FROM {{ ref('fct_service_orders_energy') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.start_date as DATE) = b.date
    WHERE contact_id IS NOT NULL
)

SELECT
    a.year_month,
    b.quantity gasoline_quantity,
    b.user_deduction gasoline_user_deduction,
    f.unique_contacts diesel_potential_users,
    h.unique_contacts diesel_actual_users,
    c.quantity diesel_quantity,
    c.user_deduction diesel_user_deduction,
    i.unique_contacts electricity_actual_users,
    g.unique_contacts electricity_potential_users,
    d.quantity electricity_quantity,
    d.user_deduction electricity_user_deduction,
    e.quantity lpg_quantity,
    e.user_deduction lpg_user_deduction
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN (SELECT * FROM energy_consumption WHERE energy_source = 'gasoline') b ON a.year_month = b.year_month
LEFT JOIN (SELECT * FROM energy_consumption WHERE energy_source = 'diesel') c ON a.year_month = c.year_month
LEFT JOIN (SELECT * FROM energy_consumption WHERE energy_source = 'electricity') d ON a.year_month = d.year_month
LEFT JOIN (SELECT * FROM energy_consumption WHERE energy_source = 'lpg') e ON a.year_month = e.year_month
LEFT JOIN (SELECT year_month, COUNT(DISTINCT contact_id) AS unique_contacts FROM potential_users WHERE vehicle_fuel_type = 'diesel' GROUP BY year_month) f ON a.year_month = f.year_month
LEFT JOIN (SELECT year_month, COUNT(DISTINCT contact_id) AS unique_contacts FROM potential_users WHERE vehicle_fuel_type = 'electric' GROUP BY year_month) g ON a.year_month = g.year_month
LEFT JOIN (SELECT year_month, COUNT(DISTINCT contact_id) AS unique_contacts FROM actual_users WHERE energy_source = 'diesel' GROUP BY year_month) h ON a.year_month = h.year_month
LEFT JOIN (SELECT year_month, COUNT(DISTINCT contact_id) AS unique_contacts FROM actual_users WHERE energy_source = 'electricity' GROUP BY year_month) i ON a.year_month = i.year_month
WHERE a.start_date <= CURRENT_DATE
ORDER BY year_month DESC
