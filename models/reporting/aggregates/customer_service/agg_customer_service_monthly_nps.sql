WITH scores AS (
    SELECT
        b.year_month,
        count(*) nr_feedbacks,
        100*countif(a.nps_response_classification = 'promoter')/count(*) promoter_share,
        100*countif(a.nps_response_classification = 'detractor')/count(*) detractor_share,
FROM {{ ref('fct_feedback_user_nps') }} a
LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.original_timestamp AS DATE) = b.date
GROUP BY year_month
)

SELECT
    a.year_month,
    a.start_date,
    a.end_date,
    (b.promoter_share - b.detractor_share) nps_score
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN scores b ON a.year_month = b.year_month
WHERE start_date < current_date
ORDER BY year_month DESC