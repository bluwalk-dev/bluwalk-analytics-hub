{{ config(
    enabled = false
) }}

SELECT
    year_month,
    COUNT(DISTINCT user_pseudo_id) AS unique_visitors,
    COUNT(DISTINCT IF(medium NOT IN ('ppc', 'paid', 'cpc', 'not set'), user_pseudo_id, NULL)) AS unique_source_organic_visitors,
    COUNT(DISTINCT IF(medium != 'not set', user_pseudo_id, NULL)) AS unique_source_total_visitors,
    COUNT(DISTINCT IF(is_new_user, user_pseudo_id, NULL)) AS unique_new_visitors,
FROM {{ ref('base_website_traffic') }} a
LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
GROUP BY year_month
ORDER BY year_month DESC