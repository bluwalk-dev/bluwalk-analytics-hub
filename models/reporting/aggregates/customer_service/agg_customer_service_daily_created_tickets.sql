SELECT
    CAST(create_date AS DATE) date,
    COUNT(*) number_of_tickets
FROM {{ ref('fct_tickets') }}
GROUP BY date
ORDER BY date DESC