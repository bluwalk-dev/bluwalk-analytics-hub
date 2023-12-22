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
    mktplace_last_activity_ridesharing
FROM {{ ref("stg_hubspot__contacts") }}
WHERE is_deleted = FALSE