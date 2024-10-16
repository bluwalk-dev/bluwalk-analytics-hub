WITH open_vehicle_contracts AS (
    SELECT *
    FROM {{ ref('inc_sm_vehicles_active') }}
    WHERE 
        active_vehicle_contracts = TRUE AND
        vehicle_contract_type = 'car_rental'
), last_account_update as (
    SELECT
        analytic_account_owner_contact_id as contact_id,
        MAX(date) last_activity
    FROM {{ ref('fct_accounting_analytic_lines') }}
    WHERE 
        analytic_account_type = 'User' AND
        move_id IS NULL
    GROUP BY analytic_account_owner_contact_id
)

SELECT DISTINCT
    x.*
    /* Troubleshooting
    ,IF(ROUND(IFNULL(y.risk_balance, 0), 2) = ROUND(IFNULL(x.balance, 0), 2), TRUE, FALSE),
    y.risk_balance,
    IF(ROUND(IFNULL(y.risk_deposit_amount, 0), 2) = ROUND(IFNULL(x.deposit, 0), 2), TRUE, FALSE),
    y.risk_deposit_amount,
    IF(ROUND(IFNULL(y.risk_net_balance, 0), 2) = ROUND(IFNULL(x.net_balance, 0), 2), TRUE, FALSE),
    y.risk_net_balance,
    IF(ROUND(IFNULL(y.risk_next_installment, 0), 2) = ROUND(IFNULL(x.next_installment, 0), 2), TRUE, FALSE),
    y.risk_next_installment,
    IF(ROUND(IFNULL(y.risk_target_balance, 0), 2) = ROUND(IFNULL(x.target_balance, 0), 2), TRUE, FALSE),
    y.risk_target_balance*/
FROM (
    SELECT
        b.user_id,
        b.user_email,
        a.deposit,
        a.outstanding_balance as balance,
        a.net_balance,
        a.accounting_balance,
        DATE_DIFF(current_date(), d.last_activity, DAY) account_idle_time
    FROM {{ ref('rpt_finances_collections_debt_report') }} a
    LEFT JOIN {{ ref('dim_users') }} b ON a.contact_id = b.contact_id
    LEFT JOIN open_vehicle_contracts c ON b.user_id = c.user_id
    LEFT JOIN last_account_update d ON a.contact_id = d.contact_id
    WHERE 
        b.user_id IS NOT NULL AND
        b.user_email IS NOT NULL 
) x
LEFT JOIN {{ ref('base_hubspot_contacts') }} y ON x.user_id = y.user_id AND x.user_email = y.email
WHERE
    y.user_id IS NOT NULL AND
    (
        ROUND(IFNULL(y.risk_balance, 0), 2) != ROUND(IFNULL(x.balance, 0), 2) OR
        ROUND(IFNULL(y.risk_deposit_amount, 0), 2) != ROUND(IFNULL(x.deposit, 0), 2) OR
        ROUND(IFNULL(y.risk_net_balance, 0), 2) != ROUND(IFNULL(x.net_balance, 0), 2) OR
        ROUND(IFNULL(y.risk_accounting_balance, 0), 2) != ROUND(IFNULL(x.accounting_balance, 0), 2) OR
        ROUND(IFNULL(y.risk_account_idle_time, 0), 2) != ROUND(IFNULL(x.account_idle_time, 0), 2)
    )