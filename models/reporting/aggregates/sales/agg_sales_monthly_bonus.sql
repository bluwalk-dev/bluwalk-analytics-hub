select
    c.year_month,
    a.bonus,
    COALESCE(b.first_payment_total, 0) as insurance_deal_value
from {{ ref('util_month_intervals') }} c
left join (
    SELECT 
        year_month, 
        SUM(bonus) bonus
    FROM {{ ref('agg_sales_monthly_pipeline_bonus') }}
    GROUP BY year_month
) a on c.year_month = a.year_month
LEFT JOIN (
    SELECT * 
    FROM {{ ref('agg_sm_monthly_insurance_agent_deal_first_payment') }}
    WHERE agent_name = 'Miguel Almeida'
) b on c.year_month = b.year_month
ORDER BY year_month DESC