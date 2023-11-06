SELECT DISTINCT
    CAST(driver_name AS STRING) AS driver_name,
    CAST(driver_id AS STRING) AS partner_account_uuid,
    CAST(driver_company_id AS STRING) AS company_id,
    CAST(driver_phone AS STRING) phone,
    CAST(period_date AS DATE) AS date,
    CAST(IFNULL(finished_orders, 0) AS INT64) AS nr_trips,
    CAST(IFNULL(online_h, 0)*60 AS NUMERIC) AS online_minutes,
    CAST(IFNULL(working_h, 0)*60 AS NUMERIC) AS working_minutes,
    CAST(acceptance_rate AS NUMERIC) AS acceptance_rate,
    CAST(utilisation AS NUMERIC) AS utilisation_rate,
    CAST(average_rating AS NUMERIC) AS average_rating,
    CAST(IFNULL(gross_revenue, 0) AS NUMERIC) AS gross_revenue,
    CAST(IFNULL(distance_driven, 0) AS NUMERIC) AS distance_driven,
    CAST(partner_id AS INT64) AS contact_id,
    CAST(account AS STRING) AS account,
    CAST(load_timestamp AS INT64) AS load_timestamp
FROM {{ source('bolt', 'driver_performance') }}
ORDER BY period_date DESC