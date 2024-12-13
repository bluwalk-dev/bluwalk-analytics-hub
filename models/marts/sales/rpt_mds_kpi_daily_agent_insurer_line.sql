WITH
policies AS (
    SELECT
        c.close_date,
        b.insurer_name,
        b.insurance_type,
        c.owner_name,
        COUNT(*) as policies_issued
    FROM {{ ref("dim_insurance_policies")}} b
    LEFT JOIN {{ ref("fct_deals")}} c ON b.insurance_policy_name = c.insurance_policy_name
    WHERE owner_name IS NOT NULL
    GROUP BY
        close_date,
        insurer_name,
        insurance_type,
        owner_name
)

SELECT
    a.date,
    b.insurer_name,
    b.insurance_type,
    b.owner_name,
    COALESCE(b.policies_issued, 0) policies_issued
FROM {{ ref("util_calendar") }} a
LEFT JOIN policies b ON a.date = b.close_date
WHERE 
    a.date BETWEEN '2023-01-01' AND CURRENT_DATE()
ORDER BY date DESC