{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

WITH

topDriverNetEarnings AS (
  SELECT
    statement,
    max(total_income) AS top_income
  FROM {{ ref('agg_wm_weekly_rideshare_income') }}
  GROUP BY statement
),

balance_without_damages AS (
    SELECT
        contact_id,
        statement,
        ROUND(SUM(amount),2) AS balance
    FROM {{ ref('fct_financial_user_transactions') }}
    WHERE product_id NOT IN (42,44)
    GROUP BY contact_id, statement
),

churn_list as (
    WITH activeUsers AS (
        SELECT DISTINCT
            user_id,
            year_week
        FROM {{ ref('agg_cm_daily_activity') }} au
        WHERE 
          user_id is not null AND 
          partner_category = 'Rideshare' AND
          partner_marketplace = 'Work' AND
          date > '2021-06-01'
        
    ), nextPaymentCycles AS (
        SELECT DISTINCT 
            year_week, 
            LEAD(year_week, 1) OVER (ORDER BY year_week) AS next_year_week
        FROM (SELECT DISTINCT year_week FROM {{ ref('util_calendar') }})
        WHERE year_week IS NOT NULL
    )
    SELECT 
        au1.user_id, 
        au1.year_week statement, 
    CASE WHEN au2.user_id IS NULL THEN 'Churned' ELSE 'Retained' END AS Status
    FROM activeUsers au1
    LEFT JOIN nextPaymentCycles npc  ON au1.year_week = npc.year_week 
    LEFT JOIN activeUsers au2  ON au1.user_id = au2.user_id AND npc.next_year_week = au2.year_week
    ORDER BY au1.user_id, au1.year_week
), 

userDimension AS (
  select distinct
    contact_id 
  from {{ ref('stg_hubspot__deals') }} a
  where is_closed_won IS true and deal_pipeline_id = '155110085' and user_id is not null
),

last_statement AS (
    SELECT CAST(MAX(period) AS INT64) AS last_statement
    FROM bluwalk-analytics-hub.staging.stg_odoo_bw_close_periods
)


SELECT
    cd.contact_id,
    cd.user_name,
    cd.user_email,
    cd.user_location,
    cd.statement,
    ne.total_income,
    RANK() OVER (PARTITION BY cd.statement ORDER BY COALESCE(ne.total_income, 0) DESC) as income_rank,
    cd.online_hours,
    cd.nr_trips,
    pd.start_date,
    pd.end_date,
    CASE WHEN cd.online_hours > 0 THEN ROUND(ne.total_income / cd.online_hours, 2) ELSE 0 END AS income_per_online_hour,
    CASE WHEN cd.nr_trips > 0 THEN ROUND(ne.total_income / cd.nr_trips, 2) ELSE 0 END AS income_per_trip,
    cd.user_id,
    db.balance AS driver_balance,
    tpd.top_income,
    CASE WHEN cd.statement = ls.last_statement THEN '' ELSE cl.Status END AS churn_status,
    CASE WHEN db.balance < 0 THEN "Low" ELSE CASE WHEN db.balance > 190 THEN "Top" ELSE "Regular" END END user_segment,
    CASE WHEN ud.contact_id IS NULL THEN 'Rented' ELSE 'Connected' END user_dimension,
    m1.total_worked_days,
    m1.percentage_over_130,
    ROUND(m2.nr_trips_5TO9/m1.total_worked_days*100,2) percentage_trips_5TO9,
    m3.acceptance_rate,
    ROUND(m4.nr_worked_weekend_days/2*100,2) percentage_worked_on_weekends
FROM {{ ref('agg_wm_weekly_rideshare_performance') }} cd
JOIN {{ ref('util_week_intervals') }} pd ON cd.statement = pd.year_week
LEFT JOIN {{ ref('agg_wm_weekly_rideshare_income') }} ne ON cd.contact_id = ne.contact_id AND cd.statement = ne.statement
LEFT JOIN balance_without_damages db ON cd.contact_id = db.contact_id AND cd.statement = db.statement
LEFT JOIN churn_list cl ON cl.user_id = cd.user_id AND cd.statement = cl.statement
LEFT JOIN userDimension ud ON ud.contact_id = cd.contact_id
LEFT JOIN topDriverNetEarnings tpd ON tpd.statement = cd.statement
/* Performance Metrics */
LEFT JOIN {{ ref('agg_wm_weekly_rideshare_earnings_over_130') }} m1 on cd.statement = m1.statement AND cd.contact_id = m1.contact_id
LEFT JOIN {{ ref('agg_wm_weekly_rideshare_trips_5TO9') }} m2 on cd.statement = m2.statement AND cd.contact_id = m2.contact_id
LEFT JOIN {{ ref('agg_wm_weekly_rideshare_acceptance_rate') }} m3 on cd.statement = m3.statement AND cd.contact_id = m3.contact_id
LEFT JOIN {{ ref('agg_wm_weekly_rideshare_work_on_weekends') }} m4 on cd.statement = m4.statement AND cd.contact_id = m4.contact_id
CROSS JOIN last_statement ls
WHERE COALESCE(ne.total_income, 0) > 0
ORDER BY statement DESC, total_income DESC