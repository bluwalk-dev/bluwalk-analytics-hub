WITH invoices_with_date_range AS (
  SELECT
    a.date AS invoice_date,  -- Invoice date
    DATE_SUB(a.date, INTERVAL 30 DAY) AS start_date,  -- Start date 30 days prior to the invoice date
    b.analytic_account_owner_contact_id,  -- Contact ID from the analytic account
    d.contact_vat,  -- VAT number of the contact
    a.year_month  -- Year and month of the invoice
  FROM {{ ref("fct_financial_user_invoices") }} a
  LEFT JOIN {{ ref("dim_accounting_analytic_accounts") }} b ON a.analytic_account_id = b.analytic_account_id
  LEFT JOIN {{ ref("dim_contacts") }} d ON a.contact_id = d.contact_id
),

-- CTE to aggregate transactions within the date range
transactions_in_date_range AS (
  SELECT
    w.contact_id,  -- Contact ID from work orders
    w.partner_category AS category,  -- Category from work orders
    SUM(w.partner_payout) AS total_amount,  -- Total amount summed up for each contact and category
    i.contact_vat,  -- VAT number from invoices
    i.invoice_date,  -- Invoice date
    i.year_month  -- Year and month of the invoice
  FROM invoices_with_date_range i
  LEFT JOIN {{ ref("fct_work_orders") }} w ON w.contact_id = i.analytic_account_owner_contact_id
  WHERE w.end_date BETWEEN i.start_date AND i.invoice_date  -- Filter transactions within the date range
  GROUP BY
    w.contact_id,
    w.partner_category,
    i.contact_vat,
    i.invoice_date,
    i.year_month
),

-- CTE to rank categories based on the total amount for each contact and year_month
ranked_categories AS (
  SELECT
    contact_vat,
    contact_id,  -- Contact ID
    category,  -- Category
    total_amount,  -- Total amount for the category
    RANK() OVER (PARTITION BY contact_id, year_month ORDER BY total_amount DESC) AS category_rank,  -- Rank categories by total amount in descending order
    year_month  -- Year and month of the invoice
  FROM transactions_in_date_range
)

-- Final query to select the top category for each contact
SELECT DISTINCT
    r.contact_vat,  -- VAT number of the contact
    r.category,  -- Category with the highest total amount
    r.year_month  -- Year and month of the invoice
FROM ranked_categories r
WHERE r.category_rank = 1  -- Select the top category (rank 1)
ORDER BY r.contact_vat DESC  -- Order by VAT number in descending order