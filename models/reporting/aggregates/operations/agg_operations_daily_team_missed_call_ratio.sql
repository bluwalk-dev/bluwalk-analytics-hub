SELECT
    date,
    team,
    CASE 
        WHEN total_valid_inbound = 0 THEN NULL
        ELSE ROUND(missed_inbound/total_valid_inbound,4)
    END missed_call_ratio
FROM {{ ref('agg_operations_daily_team_inbound_calls') }}