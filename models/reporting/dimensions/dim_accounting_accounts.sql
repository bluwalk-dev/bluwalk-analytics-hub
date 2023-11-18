-- This model is designed to simplify and restructure the data from the 'stg_odoo__account_accounts' table.
-- It selects specific columns from the source table and renames them for clarity and consistency.
-- This transformation makes the data more accessible and easier to use in downstream analyses and reporting.

SELECT
    id AS account_id,               -- Renamed for clarity: Original 'id' column is now 'account_id'.
    name AS account_name,           -- Renamed for clarity: Original 'name' column is now 'account_name'.
    code AS account_code,           -- Renamed for clarity: Original 'code' column is now 'account_code'.
    deprecated AS account_deprecated, -- Renamed to indicate if the account is deprecated.
    internal_type AS account_internal_type, -- Renamed to reflect the internal type of the account.
    internal_group AS account_internal_group -- Renamed to reflect the internal group of the account.
FROM 
    {{ ref('stg_odoo__account_accounts') }} a -- Source table: References the 'stg_odoo__account_accounts' staging model.