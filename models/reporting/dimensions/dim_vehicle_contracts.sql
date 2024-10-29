SELECT * FROM (
    SELECT 
        *
    FROM {{ ref('int_vehicle_contracts_free_loan') }}

    UNION ALL

    SELECT 
        *
    FROM {{ ref('base_drivfit_vehicle_contracts') }}
)
ORDER BY start_date DESC