with

source as (
    select
        *
    from {{ source('hubspot', 'contact') }}
),

transformation as (

    SELECT
    
        CAST(id AS INT64) hs_contact_id,
        CAST(property_firstname AS STRING) first_name,
        CAST(property_lastname AS STRING) last_name,
        CAST(property_email AS STRING) email,
        CAST(is_deleted AS BOOL) is_deleted,
        CAST(property_hs_merged_object_ids AS STRING) merged_objects,
        CAST(property_odoo_partner_id AS INT64) contact_id,
        CAST(property_odoo_user_id AS INT64) user_id,
        CAST(property_hs_calculated_phone_number AS STRING) contact_phone_nr,
        
        -- PAR properties
        CAST(property_par_has_been_on_the_program as boolean) par_has_been_on_the_program,
        CAST(property_par_currently_in_program AS BOOL) par_currently_in_program,
        CAST(property_par_days_in_program AS INT64) par_days_in_program,
        CAST(property_par_entry_date AS DATE) par_entry_date,
        CAST(property_par_re_entry_date as date) par_re_entry_date,
        CAST(property_par_exit_date AS DATE) par_exit_date,
        CAST(property_par_number_days_with_0_earnings AS INT64) par_number_days_with_0_earnings,
        CAST(property_par_number_of_days_with_earnings_below_70 AS INT64) par_number_of_days_with_earnings_below_70,
        CAST(property_par_number_of_days_with_earnings_between_70_and_130 AS INT64) par_number_of_days_with_earnings_between_70_and_130,
        CAST(property_par_sms_friday_challenge AS STRING) par_sms_friday_challenge,

        -- Marketplace Activity
        CAST(property_idle_on_work_marketplace AS NUMERIC) idle_on_work_marketplace,
        CAST(property_mktplace_last_activity_ridesharing AS DATE) mktplace_last_activity_ridesharing,
        CAST(property_active_vehicle_contracts AS boolean) active_vehicle_contracts,
        
        -- Risk Profile Data
        CAST(property_risk_balance AS NUMERIC) risk_balance,
        CAST(property_risk_deposit_amount AS NUMERIC) risk_deposit_amount,
        CAST(property_risk_net_balance AS NUMERIC) risk_net_balance,
        CAST(property_risk_next_installment AS NUMERIC) risk_next_installment,
        CAST(property_risk_target_balance AS NUMERIC) risk_target_balance,
        CAST(property_risk_accounting_balance AS NUMERIC) risk_accounting_balance

        
        

    FROM source
    WHERE _fivetran_deleted IS FALSE

)

SELECT * FROM transformation