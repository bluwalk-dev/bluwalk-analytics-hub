SELECT
    a.year_month,
    a.fleet_size as total_rented_vehicles,
    c.fleet_size as fleet_size,
    d.service_vehicles as service_vehicles,
    COALESCE(d.service_vehicles / c.fleet_size, 0) as service_percentage,
    COALESCE(a.fleet_size / c.fleet_size, 0) as rental_percentage,
    COALESCE(b.new_customers, 0) as new_contracts_companies
FROM {{ ref('agg_fleet_rental_monthly') }} a
LEFT JOIN {{ ref('agg_fleet_contracts_new_monthly') }} b ON a.year_month = b.year_month
LEFT JOIN {{ ref('agg_fleet_monthly_size') }} c ON a.year_month = c.year_month
LEFT JOIN {{ ref('agg_fleet_monthly_service') }} d ON a.year_month = d.year_month
WHERE a.year_month >= 202501