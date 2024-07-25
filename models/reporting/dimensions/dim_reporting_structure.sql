SELECT
    business as reporting_business,
    level_1 as reporting_level_1,
    level_2 as reporting_level_2,
    level_3 as reporting_level_3,
    level_4 as reporting_level_4,
    analytic_account as analytic_account_name,
    account_account as account_code
FROM {{ source('generic', 'reporting_structure') }}
WHERE company = 'Bluwalk'