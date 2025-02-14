SELECT
    date,
    user_id,
    contact_id,
    service_fee,
    vehicle_contract_type,
    vehicle_plate,
    payee_is_company,
    CASE
        WHEN service_fee = 5 AND vehicle_plate IN ('20VX96', 'AT35NN', 'AX33LC') AND vehicle_contract_type = 'free_loan' THEN TRUE
        WHEN service_fee = 5 AND vehicle_contract_type = 'free_loan' AND payee_is_company = FALSE THEN FALSE
        ELSE TRUE
    END as service_fee_compliant
FROM {{ ref('int_user_service_fee_per_day') }} a
LEFT JOIN {{ ref('dim_payment_profiles') }} b ON a.user_id = b.payment_profile_user_id
WHERE state = 'active'