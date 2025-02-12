WITH 

transaction_account_balance AS (
    SELECT
        contact_id,
        SUM(amount) as amount,
        -1 * SUM(amount_residual) as residual,
        SUM(amount_balance) as balance,
    FROM {{ ref('fct_financial_user_transaction_lines') }}
    GROUP BY
        contact_id
),
deposits AS (
    SELECT * FROM (
        SELECT
            contact_id,
            SUM(amount_untaxed) as deposit
        FROM {{ ref('fct_accounting_moves') }}
        WHERE 
            move_type IN ('Supplier Deposit', 'Supplier Deposit Refund') AND
            move_state = 'posted' AND
            financial_system = 'odoo_ce'
        
        GROUP BY contact_id)
    WHERE deposit > 0
),
user_related_partners AS (
    SELECT DISTINCT contact_id user_related_partners
    FROM {{ ref('fct_financial_user_transaction_lines') }}
    WHERE account_type = 'user'
),
last_activity as (
    SELECT
        contact_id,
        MAX(date) last_activity
    FROM {{ ref('fct_financial_user_transaction_lines') }}
    WHERE 
        account_type = 'user' AND 
        trips_id IS NOT NULL
    GROUP BY contact_id
)

SELECT * EXCEPT(user_related_partners) FROM (
    SELECT
        a.contact_id,
        a.contact_vat,
        a.contact_full_name,
        e.user_related_partners,
        f.last_activity,
        IFNULL(b.amount, 0) as analytic_balance,
        IFNULL(b.residual, 0) as accounting_balance,
        IFNULL(b.balance, 0) as outstanding_balance,
        IFNULL(d.deposit, 0) as deposit,
        IFNULL(b.balance, 0) + IFNULL(d.deposit, 0) AS net_balance
    FROM {{ ref('dim_contacts') }} a
    LEFT JOIN transaction_account_balance b ON a.contact_id = b.contact_id
    LEFT JOIN deposits d ON a.contact_id = d.contact_id
    LEFT JOIN user_related_partners e ON a.contact_id = e.user_related_partners
    LEFT JOIN last_activity f ON a.contact_id = f.contact_id
)
WHERE user_related_partners is not null
ORDER BY outstanding_balance ASC