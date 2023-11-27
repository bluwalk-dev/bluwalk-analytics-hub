SELECT 
    year_week,
    ROUND(AVG(DATETIME_DIFF(statement_close, date, HOUR) - 24),2) release_delay
FROM {{ ref('base_odoo_statement_close') }}
GROUP BY year_week
ORDER BY year_week DESC