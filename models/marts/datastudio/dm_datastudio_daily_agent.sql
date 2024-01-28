{{ config(materialized='table') }}

SELECT
    a.date,
    a.year_week,
    a.year_month,
    a.agent_name,
    a.agent_team,
    a.activation_points
FROM {{ ref("rpt_activation_daily_agent_kpis") }} a