SELECT
    a.year_month,
    a.fleet_size,
    COALESCE(b.new_customers, 0) as new_contracts_companies
FROM {{ ref('agg_fleet_rental_monthly') }} a
LEFT JOIN {{ ref('agg_fleet_contracts_new_monthly') }} b ON a.year_month = b.year_month
WHERE a.year_month >= 202501