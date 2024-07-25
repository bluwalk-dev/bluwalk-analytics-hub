SELECT
    year_month,
    reporting_business,
    reporting_level_1,
    reporting_level_2,
    reporting_level_3,
    reporting_level_4,
    -1 * SUM(balance) amount
FROM {{ ref('rpt_finances_reporting_ledger') }}
GROUP BY
    year_month,
    reporting_business,
    reporting_level_1,
    reporting_level_2,
    reporting_level_3,
    reporting_level_4
ORDER BY
    year_month,
    reporting_business,
    reporting_level_1,
    reporting_level_2,
    reporting_level_3,
    reporting_level_4