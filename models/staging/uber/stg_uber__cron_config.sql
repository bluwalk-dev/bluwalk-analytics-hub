SELECT
    'a6bedefed0135d2ca14b8a1eb7512a50' partner_key,
    1 sales_partner_id,
    CAST(orgId AS STRING) login_id,
    orgName org_name,
    org_alt_name,
    reportType report_type,
    lastLoaded last_loaded,
    folderId folder_id,
    cast(sales_tax_rate AS NUMERIC) sales_tax_rate,
    CAST(location_id AS INT64) location_id
FROM {{ source('uber', 'cron_config') }}