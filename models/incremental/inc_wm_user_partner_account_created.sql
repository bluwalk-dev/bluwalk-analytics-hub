WITH 

mercadao_accounts_created AS (
    
    SELECT
        a.user_id,
        'Mercadão' AS partner_name,
        a.deal_partner_key AS partner_key,
        DATETIME(c.last_update_ts, 'Europe/Lisbon') create_date
    FROM {{ ref("fct_deals") }} a
    LEFT JOIN {{ ref("dim_users") }} b ON a.user_id = b.user_id
    LEFT JOIN {{ ref("base_mercadao_accounts") }} c 
    ON (
        b.user_email = c.shopper_email OR 
        RIGHT(b.user_phone,9) = c.shopper_phone_number)
    WHERE 
        deal_pipeline_stage_id = '324018669' AND 
        c.partner_account_uuid IS NOT NULL
), 

uber_accounts_created AS (

    WITH accounts_creation_date AS (
        SELECT
            partner_account_uuid,
            DATETIME(MIN(extraction_ts), 'Europe/Lisbon') created_date
        FROM {{ ref("inc_uber_driver_status") }}
        GROUP BY partner_account_uuid
    )

    SELECT
        a.user_id,
        'Uber' partner_name,
        a.deal_partner_key AS partner_key,
        c.created_date
    FROM {{ ref("fct_deals") }} a
    LEFT JOIN {{ ref("dim_partners_accounts") }} b ON a.user_id = b.user_id
    LEFT JOIN accounts_creation_date c ON c.partner_account_uuid = b.partner_account_uuid
    WHERE
        a.deal_pipeline_stage_id = '294193864' AND
        b.partner_account_uuid IS NOT NULL AND
        c.created_date IS NOT NULL

)

SELECT * FROM (

    SELECT * FROM mercadao_accounts_created
    UNION ALL
    SELECT * FROM uber_accounts_created

)
ORDER BY create_date DESC