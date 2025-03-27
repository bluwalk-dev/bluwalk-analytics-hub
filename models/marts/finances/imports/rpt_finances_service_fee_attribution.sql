WITH fuel_spending AS (

    SELECT 
        b.date, 
        contact_id, 
        -1 * sum(amount) fuel_spending
    FROM {{ ref('fct_financial_user_transaction_lines') }} a
    LEFT JOIN {{ ref('util_calendar') }} b on a.payment_cycle = b.year_week
    WHERE 
        group_id = 1 AND 
        account_type = 'user'
    GROUP BY b.date, payment_cycle, contact_id
    ORDER BY contact_id desc, date desc

)


SELECT
    a.date,
    a.user_id,
    a.contact_id,
    a.service_fee,
    a.vehicle_contract_type,
    a.vehicle_plate,
    b.payee_is_company,
    CASE
        WHEN service_fee = 5 AND vehicle_plate IN ('20VX96', 'AT35NN', 'AX33LC', 'BC39HN') AND vehicle_contract_type = 'free_loan' THEN TRUE
        WHEN service_fee = 5 AND vehicle_contract_type = 'free_loan' AND payee_is_company = FALSE THEN FALSE
        ELSE TRUE
    END as service_fee_compliant,
    COALESCE(c.fuel_spending, 0) as fuel_spending
FROM {{ ref('int_user_service_fee_per_day') }} a
LEFT JOIN {{ ref('dim_payment_profiles') }} b ON a.user_id = b.payment_profile_user_id
LEFT JOIN fuel_spending c ON a.date = c.date AND a.contact_id = c.contact_id
WHERE state = 'active'