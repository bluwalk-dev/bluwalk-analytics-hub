WITH accounts AS (

    SELECT
        c.date,
        b.partner_category,
        COUNT(*) new_accounts
    FROM {{ ref("dim_partners_accounts") }} a
    LEFT JOIN {{ ref("dim_partners") }} b ON a.sales_partner_id = b.sales_partner_id
    LEFT JOIN {{ ref("util_calendar") }} c ON CAST(a.create_date AS DATE) = c.date
    WHERE a.create_date <= current_date
    GROUP BY c.date, b.partner_category
    
)

SELECT 
    a.date,
    a.year_week,
    a.year_month,
    IFNULL(b.marketing_points,0) marketing_points,
    IFNULL(a1.new_accounts, 0) new_food_accounts,
    IFNULL(a2.new_accounts, 0) new_rideshare_accounts,
    IFNULL(a3.new_accounts, 0) new_shopping_accounts,
    IFNULL(a4.new_accounts, 0) new_parcel_accounts,
    IFNULL(a5.new_accounts, 0) new_total_accounts
FROM {{ ref('util_calendar') }} a
LEFT JOIN (select date, new_accounts from accounts where partner_category = 'Food Delivery') a1 ON a.date = a1.date
LEFT JOIN (select date, new_accounts from accounts where partner_category = 'TVDE') a2 ON a.date = a2.date
LEFT JOIN (select date, new_accounts from accounts where partner_category = 'Shopping') a3 ON a.date = a3.date
LEFT JOIN (select date, new_accounts from accounts where partner_category = 'Courier') a4 ON a.date = a4.date
LEFT JOIN (select date, SUM(new_accounts) new_accounts from accounts group by date) a5 ON a.date = a5.date
LEFT JOIN {{ ref('agg_marketing_daily_marketing_points') }} b ON a.date = b.date
WHERE a.date <= current_date
ORDER BY a.date DESC