WITH

processed_referrals_v0 as (

    SELECT
        CONCAT(c.user_id, '>INVITED>', b.user_id) referral_process_code
    FROM {{ ref("fct_accounting_analytic_lines") }} a
    LEFT JOIN {{ ref("dim_users") }} b ON SPLIT(a.reference, '-')[SAFE_OFFSET(0)] = b.user_referral_code
    LEFT JOIN {{ ref("dim_users") }} c ON SPLIT(a.reference, '-')[SAFE_OFFSET(1)] = c.user_referral_code
    WHERE 
        product_id = 53 AND
        reference IS NOT NULL AND
        extract(year from date) >= 2023 AND
        analytic_account_type = 'User'
    
),
processed_referrals_v1 as (

    SELECT
        voucher_reference as referral_process_code
    FROM {{ ref("dim_vouchers") }}
    WHERE voucher_type_id = 5

),

processed_referrals AS (
    SELECT DISTINCT referral_process_code FROM (
        SELECT * FROM processed_referrals_v0
        UNION ALL
        SELECT * FROM processed_referrals_v1
    )
)

SELECT 
    a.referral_process_code,
    c.contact_id referrer_contact_id,
    a.referrer_user_name,
    a.referrer_first_work_order,
    d.contact_id referee_contact_id,
    a.referee_user_name,
    a.referee_first_work_order,
    if(b.referral_process_code is null, 'Pending', 'Created') voucher_status
FROM {{ ref("fct_user_referrals") }} a
LEFT JOIN processed_referrals b ON a.referral_process_code = b.referral_process_code
LEFT JOIN {{ ref("dim_users") }} c ON a.referrer_user_id = c.user_id
LEFT JOIN {{ ref("dim_users") }} d ON a.referee_user_id = d.user_id
WHERE 
    referrer_first_work_order IS NOT NULL AND
    referee_first_work_order IS NOT NULL