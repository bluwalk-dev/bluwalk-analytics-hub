WITH 

analytic_balance AS (
    SELECT
        analytic_account_owner_contact_id contact_id,
        ROUND(SUM(amount),2) AS balance
    FROM {{ ref('fct_accounting_analytic_lines') }}
    GROUP BY analytic_account_owner_contact_id
),
acccounting_balance AS (
    SELECT
      aaa.analytic_account_owner_contact_id contact_id, 
      -sum(am.amount_due) balance
    FROM {{ ref('fct_accounting_move_lines') }} aml
    LEFT JOIN {{ ref('dim_accounting_analytic_accounts') }} aaa on aml.analytic_account_id = aaa.analytic_account_id
    LEFT JOIN {{ ref('fct_accounting_moves') }} am on am.id = aml.move_id
    WHERE 
        aml.analytic_account_id is not null AND 
        aml.move_state = 'posted' AND 
        aaa.analytic_account_type = 'User' AND 
        am.amount_due != 0
    GROUP BY aaa.analytic_account_owner_contact_id
),
deposits AS (
    SELECT * FROM (
        SELECT
            contact_id,
            SUM(amount_untaxed) as deposit
        FROM {{ ref('fct_accounting_moves') }}
        WHERE move_type IN ('Supplier Deposit', 'Supplier Deposit Refund')
        GROUP BY contact_id)
    WHERE deposit > 0
),
user_related_partners AS (
    SELECT DISTINCT analytic_account_owner_contact_id user_related_partners
    FROM {{ ref('fct_accounting_analytic_lines') }}
    WHERE analytic_account_type = 'User'
),
last_activity as (
    SELECT
        analytic_account_owner_contact_id contact_id,
        MAX(date) last_activity
    FROM {{ ref('fct_accounting_analytic_lines') }}
    WHERE 
        analytic_account_type = 'User' AND 
        order_type = 'Work'
    GROUP BY analytic_account_owner_contact_id
)

SELECT * EXCEPT(user_related_partners) FROM (
    SELECT
        a.contact_id,
        a.contact_vat,
        a.contact_full_name,
        e.user_related_partners,
        f.last_activity,
        IFNULL(b.balance, 0) as analytic_balance,
        IFNULL(c.balance, 0) as accounting_balance,
        IFNULL(b.balance, 0) + IFNULL(c.balance, 0) as outstanding_balance,
        IFNULL(d.deposit, 0) as deposit,
        IFNULL(b.balance, 0) + IFNULL(c.balance, 0) + IFNULL(d.deposit, 0) AS net_balance
    FROM {{ ref('dim_contacts') }} a
    LEFT JOIN analytic_balance b ON a.contact_id = b.contact_id
    LEFT JOIN acccounting_balance c ON a.contact_id = c.contact_id
    LEFT JOIN deposits d ON a.contact_id = d.contact_id
    LEFT JOIN user_related_partners e ON a.contact_id = e.user_related_partners
    LEFT JOIN last_activity f ON a.contact_id = f.contact_id
)
WHERE user_related_partners is not null
ORDER BY outstanding_balance ASC