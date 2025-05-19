SELECT
    year_month,
    sum(online_trainings) as online_trainings,
    sum(onsite_trainings) as onsite_trainings,
    sum(total_trainings) as total_trainings
FROM {{ ref('agg_sm_daily_training') }}
group by year_month
