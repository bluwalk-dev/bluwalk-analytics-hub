{{ config(materialized='table') }}

SELECT
    a.date,
    a.year_week,
    a.year_month,
    a.sales_agent_name agent_name,
    a.sales_agent_team agent_team,
    a.activation_points
FROM {{ ref("rpt_activation_daily_agent_kpis") }} a