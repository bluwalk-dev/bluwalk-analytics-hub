WITH churn_value AS (
    SELECT 
        year_quarter,
        'RDS: Connected Vehicle' retention_segment,
        SUM(monthly_revenue_per_user) mrr
    FROM {{ ref('fct_quarter_params') }}
    WHERE partner_key IN (
        'a6bedefed0135d2ca14b8a1eb7512a50', -- Uber
        'e8d8ba0b559bd193d89e320f44686eee', -- Bolt
        'b60236a65364d02f8ff1c9b30d08d856', -- MDS
        '577c5ad4204a5f35d4b9c45ab285069d', -- Personal Car
        'fde59d4213a8317254ebd3f25cef278f' -- BP
    )
    GROUP BY year_quarter, retention_segment

    UNION ALL

    SELECT 
        year_quarter,
        'RDS: Vehicle Rental' retention_segment,
        SUM(monthly_revenue_per_user)
    FROM {{ ref('fct_quarter_params') }}
    WHERE partner_key IN (
        'a6bedefed0135d2ca14b8a1eb7512a50', -- Uber
        'e8d8ba0b559bd193d89e320f44686eee', -- Bolt
        '79bb6516ccd466bcb106aa89b13ad905', -- Drivfit
        'fde59d4213a8317254ebd3f25cef278f' -- BP
    )
    GROUP BY year_quarter, retention_segment

    UNION ALL

    SELECT 
        year_quarter,
        'PRC: Parcel' retention_segment,
        SUM(monthly_revenue_per_user)
    FROM {{ ref('fct_quarter_params') }}
    WHERE partner_key IN (
        'ba1a726eb8fa861fb78629b31eaba8c0' -- Correos
    )
    GROUP BY year_quarter, retention_segment

    UNION ALL

    SELECT 
        year_quarter,
        'GRC: Groceries' retention_segment,
        SUM(monthly_revenue_per_user)
    FROM {{ ref('fct_quarter_params') }}
    WHERE partner_key IN (
        '4e44a575f4fc27d5d860cd2064314c8f' -- Correos
    )
    GROUP BY year_quarter, retention_segment

    UNION ALL

    SELECT 
        year_quarter,
        'FDL: Food Delivery' retention_segment,
        SUM(monthly_revenue_per_user)
    FROM {{ ref('fct_quarter_params') }}
    WHERE partner_key IN (
        '2e42e8ab45724107a5b91ef6b389ac70', -- Uber Eats
        '30a2a39eade7e3a1e80d23b95a97fac7' -- Bolt Food
    )
    GROUP BY year_quarter, retention_segment
)

SELECT
    a.year_month,                 -- The year and month from the calendar table
    a.start_date,                 -- The start date of the month from the calendar table
    a.end_date,                  -- The end date of the month from the calendar table
    b.retention_segment,
    IFNULL(c.nr_active_users, 0) as nr_active_users,
    IFNULL(b.nr_churns,0) nr_churns,
    IFNULL(ROUND(d.churn_rate, 4), 0) as churn_rate,
    ROUND(e.mrr,2) mrr_churned_unit_value,
    ROUND((e.mrr * b.nr_churns),2) mrr_churned
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('agg_retention_monthly_retention_segment_users_churned') }} b ON a.year_month = b.year_month
LEFT JOIN {{ ref('agg_retention_monthly_retention_segment_users_active') }} c ON a.year_month = c.year_month AND b.retention_segment = c.retention_segment
LEFT JOIN {{ ref('agg_retention_monthly_retention_segment_churn_rate') }} d ON a.year_month = d.year_month AND b.retention_segment = d.retention_segment
LEFT JOIN churn_value e ON a.year_quarter = e.year_quarter AND b.retention_segment = e.retention_segment
WHERE 
    year > 2020 AND 
    start_date <= current_date()
ORDER BY a.year_month DESC