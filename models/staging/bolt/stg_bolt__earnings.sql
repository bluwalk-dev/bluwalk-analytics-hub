SELECT
    CAST(company_id AS STRING) as bolt_company_id,
    CAST(date AS date) as date,
    CAST(driver_id AS STRING) as partner_account_uuid,
    CAST(driver_name AS STRING) driver_name,
    CAST(IFNULL(earnings_per_hour_gross,0) AS NUMERIC) earnings_per_hour_gross,
    CAST(IFNULL(earnings_per_hour_net,0) AS NUMERIC) earnings_per_hour_net,
    CAST(IFNULL(gross_revenue,0) AS NUMERIC) gross_revenue,
    CAST(IFNULL(gross_revenue_app,0) AS NUMERIC) gross_revenue_app,
    CAST(IFNULL(gross_revenue_cash,0) AS NUMERIC) gross_revenue_cash,
    CAST(IFNULL(net_earnings,0) AS NUMERIC) net_earnings,
    CAST(IFNULL(cash_in_hand,0) AS NUMERIC) cash_in_hand,
    CAST(IFNULL(tips,0) AS NUMERIC) tips,
    CAST(IFNULL(bonuses,0) AS NUMERIC) bonuses,
    CAST(IFNULL(compensations,0) AS NUMERIC) compensations,
    CAST(IFNULL(cancellation_fees,0) AS NUMERIC) cancellation_fees,
    CAST(IFNULL(toll_roads,0) AS NUMERIC) toll_roads,
    CAST(IFNULL(booking_fees,0) AS NUMERIC) booking_fees,
    CAST(IFNULL(expense_booking_fees,0) AS NUMERIC) expense_booking_fees,
    CAST(IFNULL(expense_refunds,0) AS NUMERIC) expense_refunds,
    TIMESTAMP_MILLIS(load_timestamp) load_timestamp
FROM {{ source('bolt', 'earnings_history') }}
WHERE 
    gross_revenue != 0 OR
    net_earnings != 0
    