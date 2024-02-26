WITH 

user_work_start AS (
    SELECT 
        user_id, 
        min(end_date) first_work_order
    FROM {{ ref("fct_work_orders") }}
    WHERE partner_category = 'TVDE'
    GROUP BY user_id
),

referrer_users AS (
    SELECT 
        a.user_id as referrer_user_id,
        a.user_name as referrer_user_name,
        a.user_referral_code user_referrer_code, 
        b.first_work_order as referrer_first_work_order
    FROM {{ ref("dim_users") }} a
    LEFT JOIN user_work_start b ON a.user_id = b.user_id
    WHERE user_referral_code IS NOT NULL
),

referee_users AS (
    SELECT 
        a.user_id as referee_user_id,
        a.user_name as referee_user_name,
        a.user_referrer_code,
        b.first_work_order as referee_first_work_order
    FROM {{ ref("dim_users") }} a
    LEFT JOIN user_work_start b ON a.user_id = b.user_id
    WHERE user_referrer_code IS NOT NULL
)

SELECT 
    CONCAT(b.referrer_user_id, '>INVITED>', a.referee_user_id) as referral_process_code,
    -- REFERRER: This is the person who makes the invitation
    b.referrer_user_id,
    b.referrer_user_name,
    b.user_referrer_code,
    b.referrer_first_work_order,
    -- REFEREE: This is the person who has been invited
    a.referee_user_id,
    a.referee_user_name,
    a.referee_first_work_order
FROM referee_users a
LEFT JOIN referrer_users b ON a.user_referrer_code = b.user_referrer_code
WHERE b.user_referrer_code IS NOT NULL
ORDER BY referral_process_code ASC