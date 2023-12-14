SELECT
    CAST(company_id AS STRING) as bolt_company_id,
    CAST(date AS date) as date,
    CAST(driver_id AS STRING) as partner_account_uuid,
    CAST(driver_name AS STRING) driver_name,
    CAST(earnings_per_hour_gross AS NUMERIC) earnings_per_hour_gross,
    CAST(earnings_per_hour_net AS NUMERIC) earnings_per_hour_net,
    CAST(gross_revenue AS NUMERIC) gross_revenue,
    CAST(gross_revenue_app AS NUMERIC) gross_revenue_app,
    CAST(gross_revenue_cash AS NUMERIC) gross_revenue_cash,
    CAST(net_earnings AS NUMERIC) net_earnings,
    CAST(cash_in_hand AS NUMERIC) cash_in_hand,
    CAST(tips AS NUMERIC) tips,
    CAST(bonuses AS NUMERIC) bonuses,
    CAST(compensations AS NUMERIC) compensations,
    CAST(cancellation_fees AS NUMERIC) cancellation_fees,
    CAST(toll_roads AS NUMERIC) toll_roads,
    CAST(booking_fees AS NUMERIC) booking_fees,
    CAST(expense_booking_fees AS NUMERIC) expense_booking_fees,
    CAST(expense_refunds AS NUMERIC) expense_refunds,
    TIMESTAMP_MILLIS(load_timestamp) load_timestamp
FROM {{ source('bolt', 'earnings_history') }}
WHERE 
    gross_revenue != 0 OR
    net_earnings != 0
    