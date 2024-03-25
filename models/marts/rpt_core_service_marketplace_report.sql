WITH 

rental_contracts AS (
    SELECT 
        year_month, 
        avg(vehicle_rental_nr) vehicle_rental_nr 
    FROM 
        (SELECT 
            date,
            year_month,
            count(*) vehicle_rental_nr
        FROM {{ ref('agg_sm_daily_vehicle_rental_activity') }}
        GROUP BY date, year_month) a
    GROUP BY year_month
    ORDER BY year_month DESC
),

academy AS (
    SELECT c.year_month, so.date_order ,sol.*, STRPOS(sol.name,'ONLINE') online 
    FROM {{ ref('stg_odoo__sale_order_lines') }} sol
    LEFT JOIN {{ ref('stg_odoo__sale_orders') }} so ON sol.order_id = so.id
    LEFT JOIN {{ ref('util_calendar') }} c on cast(so.date_order as date) = c.date
    WHERE sol.event_id is not null and price_total = 50 and so.state = 'sale'
)

SELECT 
    a.year_month, 
    a.vehicle_rental_nr,
    b.diesel_potential_users,
    b.electricity_potential_users,
    b.diesel_actual_users,
    b.electricity_actual_users,
    b.diesel_quantity,
    b.electricity_quantity,
    b.diesel_user_deduction,
    b.electricity_user_deduction,
    c.academy_total,
    d.academy_on_site
FROM rental_contracts a
LEFT JOIN {{ ref('rpt_energy_report') }} b ON a.year_month = b.year_month
LEFT JOIN (SELECT year_month, count(*) academy_total FROM academy GROUP BY year_month) c ON c.year_month = a.year_month
LEFT JOIN (SELECT year_month, count(*) academy_on_site FROM academy WHERE online = 0 GROUP BY year_month) d ON d.year_month = a.year_month
ORDER BY year_month DESC