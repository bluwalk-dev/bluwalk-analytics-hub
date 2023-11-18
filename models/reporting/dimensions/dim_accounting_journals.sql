-- This model creates a table for accounting journals, pulling data from the stg_odoo__account_journals source table.
-- It structures journal data in a clear and concise format for further analysis.

SELECT
    id AS journal_id, -- Unique identifier for the journal
    name AS journal_name, -- Name of the journal
    code AS journal_code, -- Code representing the journal
    active AS journal_active, -- Indicates whether the journal is active
    type AS journal_type -- Type of the journal
FROM {{ ref('stg_odoo__account_journals') }}