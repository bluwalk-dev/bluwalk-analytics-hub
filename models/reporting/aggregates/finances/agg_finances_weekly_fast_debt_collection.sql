-- Selects weekly aggregated data from the finances and debt collection tables
SELECT
    a.year_week, -- Selects the year and week from the aggregated finance data
    a.amount_invoiced, -- Selects the total amount invoiced for that week
    b.amount_recovered, -- Selects the total amount recovered marked as 'fast' for that week
    ROUND(b.amount_recovered / a.amount_invoiced, 4) AS fast_collection_ratio -- Calculates the ratio of amount recovered to amount invoiced, rounded to 4 decimal places
FROM 
    {{ ref('agg_finances_weekly_debt_invoiced') }} a -- References the aggregated weekly debt invoiced table
LEFT JOIN (
    SELECT
        b2.year_week, -- Selects the year and week from the utility calendar table
        ROUND(SUM(amount_recovered), 2) AS amount_recovered -- Sums the amount recovered marked as 'fast', rounded to 2 decimal places
    FROM 
        {{ ref('fct_financial_user_debt_collection') }} b1 -- References the debt collection table
    LEFT JOIN 
        {{ ref('util_calendar') }} b2 ON b1.invoice_date = b2.date -- Joins the calendar table on invoice date to get the corresponding year and week
    WHERE 
        collection_speed = 'fast' -- Filters for records where the collection speed is marked as 'fast'
    GROUP BY 
        year_week -- Groups the results by year and week
)  b ON a.year_week = b.year_week -- Joins the subquery with the aggregated finance data on year_week
ORDER BY 
    a.year_week DESC -- Orders the results by year and week in descending order to get the most recent data first