SELECT
    user_id,
    ROUND(SUM(net_earnings), 2) AS ridesharing_yesterday_net_earnings,
    SUM(nr_trips) AS ridesharing_yesterday_nr_trips,
    ROUND((SUM(nr_trips) / SUM(CASE
            WHEN acceptance_rate != 0 THEN nr_trips / acceptance_rate
          ELSE
          NULL
        END
          )) * 100, 2) AS ridesharing_yesterday_acceptance_rate
FROM {{ ref("fct_user_rideshare_performance") }}
WHERE
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) = date
GROUP BY
    user_id