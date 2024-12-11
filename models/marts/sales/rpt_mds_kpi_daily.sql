WITH new_deals AS (
    SELECT
        create_date,
        COUNT(*) AS new_deals
    FROM {{ ref("fct_deals") }}
    WHERE deal_pipeline_id = 'default'
    GROUP BY create_date
),
close_deals AS (
    SELECT
        close_date,
        SUM(CASE WHEN is_closed_won = TRUE THEN 1 ELSE 0 END) AS won_deals,
        SUM(CASE WHEN is_closed_won = FALSE THEN 1 ELSE 0 END) AS lost_deals
    FROM {{ ref("fct_deals") }}
    WHERE deal_pipeline_id = 'default' AND is_closed = TRUE
    GROUP BY close_date
),
staged_deals_open AS (
    SELECT
        c.date,
        COUNT(*) AS staged_deals_open
    FROM {{ ref("fct_deals") }} d
    JOIN {{ ref("util_calendar") }} c 
    ON c.date BETWEEN DATE(d.insurance_entered_open) AND COALESCE(DATE(d.insurance_exited_open), CURRENT_DATE())
    WHERE d.deal_pipeline_id = 'default'
    GROUP BY c.date
),
missed_calls AS (
    SELECT 
        date,
        missed_inbound
    FROM {{ ref("agg_operations_daily_team_missed_call_ratio")}}
    WHERE team = 'Insurance'
)

SELECT 
    a.date,
    COALESCE(b.new_deals, 0) AS new_deals,
    COALESCE(c.won_deals, 0) AS won_deals,
    COALESCE(c.lost_deals, 0) AS lost_deals,
    COALESCE(d.staged_deals_open, 0) AS staged_deals_open,
    COALESCE(e.missed_inbound, 0) AS missed_calls
FROM {{ ref("util_calendar") }} a
LEFT JOIN new_deals b ON a.date = b.create_date
LEFT JOIN close_deals c ON a.date = c.close_date
LEFT JOIN staged_deals_open d ON a.date = d.date
LEFT JOIN missed_calls e ON a.date = e.date
WHERE a.date BETWEEN '2023-01-01' AND CURRENT_DATE()
ORDER BY a.date DESC