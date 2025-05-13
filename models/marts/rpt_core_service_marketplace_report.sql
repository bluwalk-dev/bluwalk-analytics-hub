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
    d.academy_on_site,
    e.insurance_revenue,
    e.insurance_won_deals
FROM rental_contracts a
LEFT JOIN {{ ref('rpt_energy_report') }} b ON a.year_month = b.year_month
LEFT JOIN (SELECT year_month, count(*) academy_total FROM academy GROUP BY year_month) c ON c.year_month = a.year_month
LEFT JOIN (SELECT year_month, count(*) academy_on_site FROM academy WHERE online = 0 GROUP BY year_month) d ON d.year_month = a.year_month
LEFT JOIN insurance e ON e.year_month = a.year_month
ORDER BY year_month DESC