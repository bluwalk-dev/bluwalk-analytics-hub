WITH
all_deals AS (
    SELECT CAST(close_date AS DATE) AS close_date, deal_pipeline_id, deal_pipeline_name as pipeline_name, deal_value as bonus FROM {{ ref('fct_deals') }} WHERE is_closed_won = TRUE
    UNION ALL
    SELECT CAST(close_date AS DATE) AS close_date, deal_pipeline_id, deal_pipeline_name as pipeline_name, deal_team_bonus as bonus FROM {{ ref('int_hubspot_drivfit_deals') }} WHERE is_closed_won = TRUE
),
insurance_won as (
  select b.policy_id, amount
  from {{ ref('stg_odoo__insurance_policy_payments') }} b
  left join (
    select
      policy_id,
      min(start_date) start_date
    from {{ ref('stg_odoo__insurance_policy_payments') }}
    group by policy_id
    ) a on a.policy_id = b.policy_id and a.start_date = b.start_date
  where a.policy_id is not null
),
bonus_current AS (
    SELECT
        a.year_month,
        b.pipeline_name,
        COUNT(*) as won_deals,
        CASE
            WHEN deal_pipeline_id IN ('155110085', '165261801') THEN 10 * COUNT(*)
            ELSE 2 * COUNT(*)
        END as bonus
    FROM {{ ref('util_calendar') }} a
    LEFT JOIN all_deals b ON a.date = b.close_date
    WHERE 
        deal_pipeline_id IN (
            '155110085', -- Personal Vehicle
            '180181749', -- TVDE Training
            '180181752', -- Physical Fuel Card
            '180111309', -- Digital Fuel Card
            '177902584', -- Parcel Delivery
            '177891797', -- Groceries Delivery
            '165261801' -- Drivfit - Drivers
        ) AND
        year_month > 202502 AND
        b.bonus > 0
    GROUP BY year_month, pipeline_name, deal_pipeline_id

    UNION ALL

    SELECT
        a.close_date_month as year_month,
        'Insurance : Vehicle Insurance' as pipeline_name,
        count(*) as won_deals,
        ROUND(SUM(c.amount) * 0.01,0) as deal_value
    FROM {{ ref('fct_deals') }} a
    LEFT JOIN {{ ref('dim_insurance_policies') }} b on a.insurance_policy_name = b.insurance_policy_name
    left join insurance_won c on b.insurance_policy_id = c.policy_id
    where 
        deal_pipeline_id = 'default' AND
        a.insurance_policy_name is not null AND
        owner_name = 'Miguel Almeida' AND
        a.close_date_month > 202502
    group by close_date_month, pipeline_name
)

SELECT * FROM bonus_current
ORDER BY year_month DESC