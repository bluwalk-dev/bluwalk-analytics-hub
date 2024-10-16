WITH merged_objects AS (
    SELECT CAST(value AS INT64) value
    FROM 
        {{ ref("stg_hubspot__contacts") }},
        UNNEST(SPLIT(merged_objects, ';')) AS value
    WHERE merged_objects IS NOT NULL
)

SELECT
    hs_contact_id,
    first_name,
    last_name,
    email,
    contact_phone_nr,
    user_id,
    contact_id,
    par_has_been_on_the_program,
    par_currently_in_program,
    par_days_in_program,
    par_entry_date,
    par_re_entry_date,
    par_exit_date,
    par_number_days_with_0_earnings,
    par_number_of_days_with_earnings_below_70,
    par_number_of_days_with_earnings_between_70_and_130,
    par_sms_friday_challenge,

    idle_work_marketplace,
    mktplace_last_activity_ridesharing,
    active_vehicle_contracts,

    risk_balance,
    risk_deposit_amount,
    risk_net_balance,
    risk_next_installment,
    risk_target_balance,
    risk_accounting_balance,
    risk_account_idle_time
    
FROM {{ ref("stg_hubspot__contacts") }}
WHERE 
    is_deleted = FALSE AND
    hs_contact_id NOT IN (SELECT * FROM merged_objects)