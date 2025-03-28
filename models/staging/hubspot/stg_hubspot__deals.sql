with

source as (
    select
        *
    from {{ source('hubspot', 'deal') }}
),

transformation as (

    select
        
        CAST (deal_id AS INT64) AS deal_id,
        
        CAST (property_dealname AS STRING) AS deal_name,
        CAST (property_partner_key AS STRING) AS deal_partner_key,
        CAST (property_deal_marketplace AS STRING) AS deal_marketplace,
        CAST (deal_pipeline_id AS STRING) AS deal_pipeline_id,
        CAST (deal_pipeline_stage_id AS STRING) AS deal_pipeline_stage_id,
        CAST (is_deleted AS BOOL) AS is_deleted,
        CAST (property_hs_is_closed_won AS BOOL) AS is_closed_won,
        CAST (property_hs_is_closed AS BOOL) AS is_closed,
        CAST (property_odoo_partner_id AS INT64) AS contact_id,
        CAST (property_odoo_user_id AS INT64) AS user_id,
        CAST (owner_id AS INT64) AS owner_id,
        CAST (property_amount AS FLOAT64) AS deal_value,

        /* Marketing Parameters */
        CAST(property_deal_source_url AS STRING) AS source_url,
        CAST(property_google_analytics_client_id AS STRING) AS google_analytics_client_id,
        CAST(property_facebook_click_id AS STRING) AS facebook_click_id,
        CAST(property_google_ad_click_id AS STRING) AS google_ad_click_id,
        CAST(property_hs_analytics_latest_source AS STRING) AS latest_source,
        CAST(property_hs_analytics_source AS STRING) AS original_source,

        /* Fuel and Energy */
        CAST (property_fuel_energy_card_number AS STRING) AS energy_card_name,
        CAST (property_fen_partner_name AS STRING) AS energy_partner_name,

        /* Training */
        CAST (property_ttv_partner_name AS STRING) AS training_tvde_partner_name,

        /* Drivfit */
        CAST (property_vdf_licenseplate AS STRING) AS vehicle_rental_license_plate,
        CAST (property_vdf_rate_description AS STRING) AS vehicle_rental_rate,
        CAST (property_vdf_booking_pickup_date AS STRING) AS vehicle_rental_pickup_date,
        CAST (property_vdf_booking_pickup_station AS STRING) AS vehicle_rental_pickup_station,
        CAST (property_vdf_booking_type AS STRING) AS vehicle_rental_booking_type,

        /* Insurance */
        CAST(property_ivi_policy_odoo_name AS STRING) AS insurance_policy_name,
        CAST(property_ivi_insurance_insurer AS INT64) AS insurance_insurer_id,
        CASE 
            WHEN SAFE_CAST(property_ivi_policy_type AS INT64) IS NULL THEN 1
            ELSE CAST(property_ivi_policy_type AS INT64)
        END AS insurance_policy_type_id,
        CAST(property_insurance_current_annual_price AS NUMERIC) AS insurance_annual_premium,
        
        COALESCE(
            CAST(property_hs_date_entered_appointmentscheduled AS TIMESTAMP),
            CAST(property_hs_v_2_date_entered_appointmentscheduled AS TIMESTAMP)
        ) AS insurance_entered_open,
        COALESCE(
            CAST(property_hs_date_exited_appointmentscheduled AS TIMESTAMP),
            CAST(property_hs_v_2_date_exited_appointmentscheduled AS TIMESTAMP)
        ) AS insurance_exited_open,
        
        COALESCE(
            CAST(property_hs_date_entered_contractsent AS TIMESTAMP),
            CAST(property_hs_v_2_date_entered_contractsent AS TIMESTAMP)
        ) AS insurance_entered_accepted,
        COALESCE(
            CAST(property_hs_date_exited_contractsent AS TIMESTAMP),
            CAST(property_hs_v_2_date_exited_contractsent AS TIMESTAMP)
        ) AS insurance_exited_accepted,

        COALESCE(
            CAST(property_hs_date_entered_decisionmakerboughtin AS TIMESTAMP),
            CAST(property_hs_v_2_date_entered_decisionmakerboughtin AS TIMESTAMP)
        ) AS insurance_entered_proposal_sent,
        COALESCE(
            CAST(property_hs_date_exited_decisionmakerboughtin AS TIMESTAMP),
            CAST(property_hs_v_2_date_exited_decisionmakerboughtin AS TIMESTAMP)
        ) AS insurance_exited_proposal_sent,
        
        CASE WHEN property_proposal_fidelidade IS NOT NULL THEN TRUE ELSE FALSE END as insurance_quote_fidelidade,
        CASE WHEN property_proposal_allianz IS NOT NULL THEN TRUE ELSE FALSE END as insurance_quote_allianz,
        CASE WHEN property_proposal_lusitania IS NOT NULL THEN TRUE ELSE FALSE END as insurance_quote_lusitania,
        CASE WHEN property_proposal_tranquilidade IS NOT NULL THEN TRUE ELSE FALSE END as insurance_quote_tranquilidade,

        /* Personal Vehicle */
        property_lost_reason_personal_car AS personal_vehicle_lost_reason,

        CAST (IFNULL(property_vdf_licenseplate, property_vpc_licenseplate) AS STRING) AS vehicle_plate,
        CAST (property_closedate AS TIMESTAMP) AS close_date,
        CAST (property_createdate AS TIMESTAMP) AS create_date

    from source
    where _fivetran_deleted is false

)

select * from transformation

/* SELECT
  concat('CAST (',column_name, ' AS ',data_type, ') AS ', replace(column_name,'property_',''),','),
FROM
  `bluwalk-data-warehouse.hubspot.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'deal' and column_name NOT IN ('_fivetran_synced', '_fivetran_deleted')
ORDER BY
  ordinal_position; 
  
f0_
"CAST (deal_id AS INT64) AS deal_id,"
"CAST (deal_pipeline_id AS STRING) AS deal_pipeline_id,"
"CAST (deal_pipeline_stage_id AS STRING) AS deal_pipeline_stage_id,"
"CAST (is_deleted AS BOOL) AS is_deleted,"
"CAST (owner_id AS INT64) AS owner_id,"
"CAST (portal_id AS INT64) AS portal_id,"
"CAST (property_appointment_time AS STRING) AS appointment_time,"
"CAST (property_closedate AS TIMESTAMP) AS closedate,"
"CAST (property_createdate AS TIMESTAMP) AS createdate,"
"CAST (property_customer_location AS STRING) AS customer_location,"
"CAST (property_customer_name AS STRING) AS customer_name,"
"CAST (property_customer_tax_id AS STRING) AS customer_tax_id,"
"CAST (property_days_to_close AS FLOAT64) AS days_to_close,"
"CAST (property_flag_bridge AS STRING) AS flag_bridge,"
"CAST (property_gmc_training_date AS STRING) AS gmc_training_date,"
"CAST (property_gmc_training_location AS STRING) AS gmc_training_location,"
"CAST (property_gmc_training_outcome AS STRING) AS gmc_training_outcome,"
"CAST (property_hs_all_accessible_team_ids AS INT64) AS hs_all_accessible_team_ids,"
"CAST (property_hs_all_owner_ids AS STRING) AS hs_all_owner_ids,"
"CAST (property_hs_all_team_ids AS INT64) AS hs_all_team_ids,"
"CAST (property_hs_analytics_latest_source AS STRING) AS hs_analytics_latest_source,"
"CAST (property_hs_analytics_latest_source_contact AS STRING) AS hs_analytics_latest_source_contact,"
"CAST (property_hs_analytics_latest_source_data_1 AS STRING) AS hs_analytics_latest_source_data_1,"
"CAST (property_hs_analytics_latest_source_data_1_contact AS STRING) AS hs_analytics_latest_source_data_1_contact,"
"CAST (property_hs_analytics_latest_source_data_2 AS STRING) AS hs_analytics_latest_source_data_2,"
"CAST (property_hs_analytics_latest_source_data_2_contact AS STRING) AS hs_analytics_latest_source_data_2_contact,"
"CAST (property_hs_analytics_latest_source_timestamp AS TIMESTAMP) AS hs_analytics_latest_source_timestamp,"
"CAST (property_hs_analytics_latest_source_timestamp_contact AS TIMESTAMP) AS hs_analytics_latest_source_timestamp_contact,"
"CAST (property_hs_analytics_source AS STRING) AS hs_analytics_source,"
"CAST (property_hs_analytics_source_data_1 AS STRING) AS hs_analytics_source_data_1,"
"CAST (property_hs_analytics_source_data_2 AS STRING) AS hs_analytics_source_data_2,"
"CAST (property_hs_closed_amount AS FLOAT64) AS hs_closed_amount,"
"CAST (property_hs_closed_amount_in_home_currency AS FLOAT64) AS hs_closed_amount_in_home_currency,"
"CAST (property_hs_closed_won_count AS FLOAT64) AS hs_closed_won_count,"
"CAST (property_hs_closed_won_date AS TIMESTAMP) AS hs_closed_won_date,"
"CAST (property_hs_created_by_user_id AS FLOAT64) AS hs_created_by_user_id,"
"CAST (property_hs_createdate AS TIMESTAMP) AS hs_createdate,"
"CAST (property_hs_date_entered_294191047 AS TIMESTAMP) AS hs_date_entered_294191047,"
"CAST (property_hs_date_entered_294191049 AS TIMESTAMP) AS hs_date_entered_294191049,"
"CAST (property_hs_date_entered_294191050 AS TIMESTAMP) AS hs_date_entered_294191050,"
"CAST (property_hs_date_entered_294191051 AS TIMESTAMP) AS hs_date_entered_294191051,"
"CAST (property_hs_date_entered_294191052 AS TIMESTAMP) AS hs_date_entered_294191052,"
"CAST (property_hs_date_entered_294191053 AS TIMESTAMP) AS hs_date_entered_294191053,"
"CAST (property_hs_date_entered_294193862 AS TIMESTAMP) AS hs_date_entered_294193862,"
"CAST (property_hs_date_entered_294193863 AS TIMESTAMP) AS hs_date_entered_294193863,"
"CAST (property_hs_date_entered_294193864 AS TIMESTAMP) AS hs_date_entered_294193864,"
"CAST (property_hs_date_entered_294193865 AS TIMESTAMP) AS hs_date_entered_294193865,"
"CAST (property_hs_date_entered_294193866 AS TIMESTAMP) AS hs_date_entered_294193866,"
"CAST (property_hs_date_entered_294193867 AS TIMESTAMP) AS hs_date_entered_294193867,"
"CAST (property_hs_date_entered_294193868 AS TIMESTAMP) AS hs_date_entered_294193868,"
"CAST (property_hs_date_entered_294193870 AS TIMESTAMP) AS hs_date_entered_294193870,"
"CAST (property_hs_date_entered_294193871 AS TIMESTAMP) AS hs_date_entered_294193871,"
"CAST (property_hs_date_entered_295902923 AS TIMESTAMP) AS hs_date_entered_295902923,"
"CAST (property_hs_date_entered_306128613 AS TIMESTAMP) AS hs_date_entered_306128613,"
"CAST (property_hs_date_entered_307250898 AS TIMESTAMP) AS hs_date_entered_307250898,"
"CAST (property_hs_date_entered_307250899 AS TIMESTAMP) AS hs_date_entered_307250899,"
"CAST (property_hs_date_entered_307250900 AS TIMESTAMP) AS hs_date_entered_307250900,"
"CAST (property_hs_date_entered_307257314 AS TIMESTAMP) AS hs_date_entered_307257314,"
"CAST (property_hs_date_entered_307257315 AS TIMESTAMP) AS hs_date_entered_307257315,"
"CAST (property_hs_date_entered_307257316 AS TIMESTAMP) AS hs_date_entered_307257316,"
"CAST (property_hs_date_entered_307257317 AS TIMESTAMP) AS hs_date_entered_307257317,"
"CAST (property_hs_date_entered_307257318 AS TIMESTAMP) AS hs_date_entered_307257318,"
"CAST (property_hs_date_entered_307257319 AS TIMESTAMP) AS hs_date_entered_307257319,"
"CAST (property_hs_date_entered_307257329 AS TIMESTAMP) AS hs_date_entered_307257329,"
"CAST (property_hs_date_entered_310796490 AS TIMESTAMP) AS hs_date_entered_310796490,"
"CAST (property_hs_date_entered_310926831 AS TIMESTAMP) AS hs_date_entered_310926831,"
"CAST (property_hs_date_entered_310926832 AS TIMESTAMP) AS hs_date_entered_310926832,"
"CAST (property_hs_date_entered_310960365 AS TIMESTAMP) AS hs_date_entered_310960365,"
"CAST (property_hs_date_entered_310960366 AS TIMESTAMP) AS hs_date_entered_310960366,"
"CAST (property_hs_date_entered_310960367 AS TIMESTAMP) AS hs_date_entered_310960367,"
"CAST (property_hs_date_entered_310960368 AS TIMESTAMP) AS hs_date_entered_310960368,"
"CAST (property_hs_date_entered_310960369 AS TIMESTAMP) AS hs_date_entered_310960369,"
"CAST (property_hs_date_entered_310960370 AS TIMESTAMP) AS hs_date_entered_310960370,"
"CAST (property_hs_date_entered_310960371 AS TIMESTAMP) AS hs_date_entered_310960371,"
"CAST (property_hs_date_entered_322675918 AS TIMESTAMP) AS hs_date_entered_322675918,"
"CAST (property_hs_date_entered_322675919 AS TIMESTAMP) AS hs_date_entered_322675919,"
"CAST (property_hs_date_entered_322675920 AS TIMESTAMP) AS hs_date_entered_322675920,"
"CAST (property_hs_date_entered_322675921 AS TIMESTAMP) AS hs_date_entered_322675921,"
"CAST (property_hs_date_entered_322675924 AS TIMESTAMP) AS hs_date_entered_322675924,"
"CAST (property_hs_date_entered_322754778 AS TIMESTAMP) AS hs_date_entered_322754778,"
"CAST (property_hs_date_entered_322754803 AS TIMESTAMP) AS hs_date_entered_322754803,"
"CAST (property_hs_date_entered_322755002 AS TIMESTAMP) AS hs_date_entered_322755002,"
"CAST (property_hs_date_entered_322755003 AS TIMESTAMP) AS hs_date_entered_322755003,"
"CAST (property_hs_date_entered_322755004 AS TIMESTAMP) AS hs_date_entered_322755004,"
"CAST (property_hs_date_entered_322755005 AS TIMESTAMP) AS hs_date_entered_322755005,"
"CAST (property_hs_date_entered_322755008 AS TIMESTAMP) AS hs_date_entered_322755008,"
"CAST (property_hs_date_entered_322810852 AS TIMESTAMP) AS hs_date_entered_322810852,"
"CAST (property_hs_date_entered_322810853 AS TIMESTAMP) AS hs_date_entered_322810853,"
"CAST (property_hs_date_entered_322810873 AS TIMESTAMP) AS hs_date_entered_322810873,"
"CAST (property_hs_date_entered_322811066 AS TIMESTAMP) AS hs_date_entered_322811066,"
"CAST (property_hs_date_entered_322811067 AS TIMESTAMP) AS hs_date_entered_322811067,"
"CAST (property_hs_date_entered_322811068 AS TIMESTAMP) AS hs_date_entered_322811068,"
"CAST (property_hs_date_entered_322811071 AS TIMESTAMP) AS hs_date_entered_322811071,"
"CAST (property_hs_date_entered_324018667 AS TIMESTAMP) AS hs_date_entered_324018667,"
"CAST (property_hs_date_entered_324018668 AS TIMESTAMP) AS hs_date_entered_324018668,"
"CAST (property_hs_date_entered_324018669 AS TIMESTAMP) AS hs_date_entered_324018669,"
"CAST (property_hs_date_entered_327026393 AS TIMESTAMP) AS hs_date_entered_327026393,"
"CAST (property_hs_date_entered_327026394 AS TIMESTAMP) AS hs_date_entered_327026394,"
"CAST (property_hs_date_entered_327026395 AS TIMESTAMP) AS hs_date_entered_327026395,"
"CAST (property_hs_date_entered_appointmentscheduled AS TIMESTAMP) AS hs_date_entered_appointmentscheduled,"
"CAST (property_hs_date_entered_closedlost AS TIMESTAMP) AS hs_date_entered_closedlost,"
"CAST (property_hs_date_entered_closedwon AS TIMESTAMP) AS hs_date_entered_closedwon,"
"CAST (property_hs_date_entered_contractsent AS TIMESTAMP) AS hs_date_entered_contractsent,"
"CAST (property_hs_date_entered_decisionmakerboughtin AS TIMESTAMP) AS hs_date_entered_decisionmakerboughtin,"
"CAST (property_hs_date_entered_presentationscheduled AS TIMESTAMP) AS hs_date_entered_presentationscheduled,"
"CAST (property_hs_date_exited_294191047 AS TIMESTAMP) AS hs_date_exited_294191047,"
"CAST (property_hs_date_exited_294191049 AS TIMESTAMP) AS hs_date_exited_294191049,"
"CAST (property_hs_date_exited_294191050 AS TIMESTAMP) AS hs_date_exited_294191050,"
"CAST (property_hs_date_exited_294191051 AS TIMESTAMP) AS hs_date_exited_294191051,"
"CAST (property_hs_date_exited_294191052 AS TIMESTAMP) AS hs_date_exited_294191052,"
"CAST (property_hs_date_exited_294193862 AS TIMESTAMP) AS hs_date_exited_294193862,"
"CAST (property_hs_date_exited_294193863 AS TIMESTAMP) AS hs_date_exited_294193863,"
"CAST (property_hs_date_exited_294193864 AS TIMESTAMP) AS hs_date_exited_294193864,"
"CAST (property_hs_date_exited_294193865 AS TIMESTAMP) AS hs_date_exited_294193865,"
"CAST (property_hs_date_exited_294193866 AS TIMESTAMP) AS hs_date_exited_294193866,"
"CAST (property_hs_date_exited_294193870 AS TIMESTAMP) AS hs_date_exited_294193870,"
"CAST (property_hs_date_exited_295902923 AS TIMESTAMP) AS hs_date_exited_295902923,"
"CAST (property_hs_date_exited_306128613 AS TIMESTAMP) AS hs_date_exited_306128613,"
"CAST (property_hs_date_exited_307250898 AS TIMESTAMP) AS hs_date_exited_307250898,"
"CAST (property_hs_date_exited_307250899 AS TIMESTAMP) AS hs_date_exited_307250899,"
"CAST (property_hs_date_exited_307250900 AS TIMESTAMP) AS hs_date_exited_307250900,"
"CAST (property_hs_date_exited_307257314 AS TIMESTAMP) AS hs_date_exited_307257314,"
"CAST (property_hs_date_exited_307257315 AS TIMESTAMP) AS hs_date_exited_307257315,"
"CAST (property_hs_date_exited_307257316 AS TIMESTAMP) AS hs_date_exited_307257316,"
"CAST (property_hs_date_exited_307257317 AS TIMESTAMP) AS hs_date_exited_307257317,"
"CAST (property_hs_date_exited_307257318 AS TIMESTAMP) AS hs_date_exited_307257318,"
"CAST (property_hs_date_exited_307257329 AS TIMESTAMP) AS hs_date_exited_307257329,"
"CAST (property_hs_date_exited_310796490 AS TIMESTAMP) AS hs_date_exited_310796490,"
"CAST (property_hs_date_exited_310926831 AS TIMESTAMP) AS hs_date_exited_310926831,"
"CAST (property_hs_date_exited_310926832 AS TIMESTAMP) AS hs_date_exited_310926832,"
"CAST (property_hs_date_exited_310960365 AS TIMESTAMP) AS hs_date_exited_310960365,"
"CAST (property_hs_date_exited_310960366 AS TIMESTAMP) AS hs_date_exited_310960366,"
"CAST (property_hs_date_exited_310960367 AS TIMESTAMP) AS hs_date_exited_310960367,"
"CAST (property_hs_date_exited_310960368 AS TIMESTAMP) AS hs_date_exited_310960368,"
"CAST (property_hs_date_exited_310960369 AS TIMESTAMP) AS hs_date_exited_310960369,"
"CAST (property_hs_date_exited_322675918 AS TIMESTAMP) AS hs_date_exited_322675918,"
"CAST (property_hs_date_exited_322675919 AS TIMESTAMP) AS hs_date_exited_322675919,"
"CAST (property_hs_date_exited_322675920 AS TIMESTAMP) AS hs_date_exited_322675920,"
"CAST (property_hs_date_exited_322675921 AS TIMESTAMP) AS hs_date_exited_322675921,"
"CAST (property_hs_date_exited_322754778 AS TIMESTAMP) AS hs_date_exited_322754778,"
"CAST (property_hs_date_exited_322755002 AS TIMESTAMP) AS hs_date_exited_322755002,"
"CAST (property_hs_date_exited_322755003 AS TIMESTAMP) AS hs_date_exited_322755003,"
"CAST (property_hs_date_exited_322755004 AS TIMESTAMP) AS hs_date_exited_322755004,"
"CAST (property_hs_date_exited_322755005 AS TIMESTAMP) AS hs_date_exited_322755005,"
"CAST (property_hs_date_exited_322810852 AS TIMESTAMP) AS hs_date_exited_322810852,"
"CAST (property_hs_date_exited_322810873 AS TIMESTAMP) AS hs_date_exited_322810873,"
"CAST (property_hs_date_exited_322811066 AS TIMESTAMP) AS hs_date_exited_322811066,"
"CAST (property_hs_date_exited_322811067 AS TIMESTAMP) AS hs_date_exited_322811067,"
"CAST (property_hs_date_exited_322811068 AS TIMESTAMP) AS hs_date_exited_322811068,"
"CAST (property_hs_date_exited_324018667 AS TIMESTAMP) AS hs_date_exited_324018667,"
"CAST (property_hs_date_exited_324018668 AS TIMESTAMP) AS hs_date_exited_324018668,"
"CAST (property_hs_date_exited_327026393 AS TIMESTAMP) AS hs_date_exited_327026393,"
"CAST (property_hs_date_exited_327026394 AS TIMESTAMP) AS hs_date_exited_327026394,"
"CAST (property_hs_date_exited_appointmentscheduled AS TIMESTAMP) AS hs_date_exited_appointmentscheduled,"
"CAST (property_hs_date_exited_contractsent AS TIMESTAMP) AS hs_date_exited_contractsent,"
"CAST (property_hs_date_exited_decisionmakerboughtin AS TIMESTAMP) AS hs_date_exited_decisionmakerboughtin,"
"CAST (property_hs_date_exited_presentationscheduled AS TIMESTAMP) AS hs_date_exited_presentationscheduled,"
"CAST (property_hs_days_to_close_raw AS FLOAT64) AS hs_days_to_close_raw,"
"CAST (property_hs_deal_stage_probability AS FLOAT64) AS hs_deal_stage_probability,"
"CAST (property_hs_deal_stage_probability_shadow AS FLOAT64) AS hs_deal_stage_probability_shadow,"
"CAST (property_hs_is_closed AS BOOL) AS hs_is_closed,"
"CAST (property_hs_is_closed_won AS BOOL) AS hs_is_closed_won,"
"CAST (property_hs_is_deal_split AS BOOL) AS hs_is_deal_split,"
"CAST (property_hs_is_open_count AS FLOAT64) AS hs_is_open_count,"
"CAST (property_hs_lastmodifieddate AS TIMESTAMP) AS hs_lastmodifieddate,"
"CAST (property_hs_merged_object_ids AS STRING) AS hs_merged_object_ids,"
"CAST (property_hs_num_associated_active_deal_registrations AS FLOAT64) AS hs_num_associated_active_deal_registrations,"
"CAST (property_hs_num_associated_deal_registrations AS FLOAT64) AS hs_num_associated_deal_registrations,"
"CAST (property_hs_num_associated_deal_splits AS FLOAT64) AS hs_num_associated_deal_splits,"
"CAST (property_hs_num_of_associated_line_items AS FLOAT64) AS hs_num_of_associated_line_items,"
"CAST (property_hs_num_target_accounts AS FLOAT64) AS hs_num_target_accounts,"
"CAST (property_hs_object_id AS FLOAT64) AS hs_object_id,"
"CAST (property_hs_object_source AS STRING) AS hs_object_source,"
"CAST (property_hs_object_source_id AS STRING) AS hs_object_source_id,"
"CAST (property_hs_object_source_user_id AS FLOAT64) AS hs_object_source_user_id,"
"CAST (property_hs_priority AS STRING) AS hs_priority,"
"CAST (property_hs_read_only AS BOOL) AS hs_read_only,"
"CAST (property_hs_sales_email_last_replied AS TIMESTAMP) AS hs_sales_email_last_replied,"
"CAST (property_hs_time_in_294191047 AS FLOAT64) AS hs_time_in_294191047,"
"CAST (property_hs_time_in_294191049 AS FLOAT64) AS hs_time_in_294191049,"
"CAST (property_hs_time_in_294191050 AS FLOAT64) AS hs_time_in_294191050,"
"CAST (property_hs_time_in_294191051 AS FLOAT64) AS hs_time_in_294191051,"
"CAST (property_hs_time_in_294191052 AS FLOAT64) AS hs_time_in_294191052,"
"CAST (property_hs_time_in_294191053 AS FLOAT64) AS hs_time_in_294191053,"
"CAST (property_hs_time_in_294193862 AS FLOAT64) AS hs_time_in_294193862,"
"CAST (property_hs_time_in_294193863 AS FLOAT64) AS hs_time_in_294193863,"
"CAST (property_hs_time_in_294193864 AS FLOAT64) AS hs_time_in_294193864,"
"CAST (property_hs_time_in_294193865 AS FLOAT64) AS hs_time_in_294193865,"
"CAST (property_hs_time_in_294193866 AS FLOAT64) AS hs_time_in_294193866,"
"CAST (property_hs_time_in_294193867 AS FLOAT64) AS hs_time_in_294193867,"
"CAST (property_hs_time_in_294193868 AS FLOAT64) AS hs_time_in_294193868,"
"CAST (property_hs_time_in_294193870 AS FLOAT64) AS hs_time_in_294193870,"
"CAST (property_hs_time_in_294193871 AS FLOAT64) AS hs_time_in_294193871,"
"CAST (property_hs_time_in_295902923 AS FLOAT64) AS hs_time_in_295902923,"
"CAST (property_hs_time_in_306128613 AS FLOAT64) AS hs_time_in_306128613,"
"CAST (property_hs_time_in_307250898 AS FLOAT64) AS hs_time_in_307250898,"
"CAST (property_hs_time_in_307250899 AS FLOAT64) AS hs_time_in_307250899,"
"CAST (property_hs_time_in_307250900 AS FLOAT64) AS hs_time_in_307250900,"
"CAST (property_hs_time_in_307257314 AS FLOAT64) AS hs_time_in_307257314,"
"CAST (property_hs_time_in_307257315 AS FLOAT64) AS hs_time_in_307257315,"
"CAST (property_hs_time_in_307257316 AS FLOAT64) AS hs_time_in_307257316,"
"CAST (property_hs_time_in_307257317 AS FLOAT64) AS hs_time_in_307257317,"
"CAST (property_hs_time_in_307257318 AS FLOAT64) AS hs_time_in_307257318,"
"CAST (property_hs_time_in_307257319 AS FLOAT64) AS hs_time_in_307257319,"
"CAST (property_hs_time_in_307257329 AS FLOAT64) AS hs_time_in_307257329,"
"CAST (property_hs_time_in_310796490 AS FLOAT64) AS hs_time_in_310796490,"
"CAST (property_hs_time_in_310926831 AS FLOAT64) AS hs_time_in_310926831,"
"CAST (property_hs_time_in_310926832 AS FLOAT64) AS hs_time_in_310926832,"
"CAST (property_hs_time_in_310960365 AS FLOAT64) AS hs_time_in_310960365,"
"CAST (property_hs_time_in_310960366 AS FLOAT64) AS hs_time_in_310960366,"
"CAST (property_hs_time_in_310960367 AS FLOAT64) AS hs_time_in_310960367,"
"CAST (property_hs_time_in_310960368 AS FLOAT64) AS hs_time_in_310960368,"
"CAST (property_hs_time_in_310960369 AS FLOAT64) AS hs_time_in_310960369,"
"CAST (property_hs_time_in_310960370 AS FLOAT64) AS hs_time_in_310960370,"
"CAST (property_hs_time_in_310960371 AS FLOAT64) AS hs_time_in_310960371,"
"CAST (property_hs_time_in_322675918 AS FLOAT64) AS hs_time_in_322675918,"
"CAST (property_hs_time_in_322675919 AS FLOAT64) AS hs_time_in_322675919,"
"CAST (property_hs_time_in_322675920 AS FLOAT64) AS hs_time_in_322675920,"
"CAST (property_hs_time_in_322675921 AS FLOAT64) AS hs_time_in_322675921,"
"CAST (property_hs_time_in_322675924 AS FLOAT64) AS hs_time_in_322675924,"
"CAST (property_hs_time_in_322754778 AS FLOAT64) AS hs_time_in_322754778,"
"CAST (property_hs_time_in_322754803 AS FLOAT64) AS hs_time_in_322754803,"
"CAST (property_hs_time_in_322755002 AS FLOAT64) AS hs_time_in_322755002,"
"CAST (property_hs_time_in_322755003 AS FLOAT64) AS hs_time_in_322755003,"
"CAST (property_hs_time_in_322755004 AS FLOAT64) AS hs_time_in_322755004,"
"CAST (property_hs_time_in_322755005 AS FLOAT64) AS hs_time_in_322755005,"
"CAST (property_hs_time_in_322755008 AS FLOAT64) AS hs_time_in_322755008,"
"CAST (property_hs_time_in_322810852 AS FLOAT64) AS hs_time_in_322810852,"
"CAST (property_hs_time_in_322810853 AS FLOAT64) AS hs_time_in_322810853,"
"CAST (property_hs_time_in_322810873 AS FLOAT64) AS hs_time_in_322810873,"
"CAST (property_hs_time_in_322811066 AS FLOAT64) AS hs_time_in_322811066,"
"CAST (property_hs_time_in_322811067 AS FLOAT64) AS hs_time_in_322811067,"
"CAST (property_hs_time_in_322811068 AS FLOAT64) AS hs_time_in_322811068,"
"CAST (property_hs_time_in_322811071 AS FLOAT64) AS hs_time_in_322811071,"
"CAST (property_hs_time_in_324018667 AS FLOAT64) AS hs_time_in_324018667,"
"CAST (property_hs_time_in_324018668 AS FLOAT64) AS hs_time_in_324018668,"
"CAST (property_hs_time_in_324018669 AS FLOAT64) AS hs_time_in_324018669,"
"CAST (property_hs_time_in_327026393 AS FLOAT64) AS hs_time_in_327026393,"
"CAST (property_hs_time_in_327026394 AS FLOAT64) AS hs_time_in_327026394,"
"CAST (property_hs_time_in_327026395 AS FLOAT64) AS hs_time_in_327026395,"
"CAST (property_hs_time_in_appointmentscheduled AS FLOAT64) AS hs_time_in_appointmentscheduled,"
"CAST (property_hs_time_in_closedlost AS FLOAT64) AS hs_time_in_closedlost,"
"CAST (property_hs_time_in_closedwon AS FLOAT64) AS hs_time_in_closedwon,"
"CAST (property_hs_time_in_contractsent AS FLOAT64) AS hs_time_in_contractsent,"
"CAST (property_hs_time_in_decisionmakerboughtin AS FLOAT64) AS hs_time_in_decisionmakerboughtin,"
"CAST (property_hs_time_in_presentationscheduled AS FLOAT64) AS hs_time_in_presentationscheduled,"
"CAST (property_hs_updated_by_user_id AS FLOAT64) AS hs_updated_by_user_id,"
"CAST (property_hs_user_ids_of_all_owners AS STRING) AS hs_user_ids_of_all_owners,"
"CAST (property_hs_was_imported AS BOOL) AS hs_was_imported,"
"CAST (property_hubspot_owner_assigneddate AS TIMESTAMP) AS hubspot_owner_assigneddate,"
"CAST (property_hubspot_team_id AS INT64) AS hubspot_team_id,"
"CAST (property_ivi_dua AS STRING) AS ivi_dua,"
"CAST (property_ivi_greencard AS STRING) AS ivi_greencard,"
"CAST (property_ivi_insurance_coverage AS STRING) AS ivi_insurance_coverage,"
"CAST (property_ivi_licenseplate AS STRING) AS ivi_licenseplate,"
"CAST (property_ivi_paymentdate AS TIMESTAMP) AS ivi_paymentdate,"
"CAST (property_notes_last_contacted AS TIMESTAMP) AS notes_last_contacted,"
"CAST (property_notes_last_updated AS TIMESTAMP) AS notes_last_updated,"
"CAST (property_notes_next_activity_date AS TIMESTAMP) AS notes_next_activity_date,"
"CAST (property_num_associated_contacts AS FLOAT64) AS num_associated_contacts,"
"CAST (property_num_contacted_notes AS FLOAT64) AS num_contacted_notes,"
"CAST (property_num_notes AS FLOAT64) AS num_notes,"
"CAST (property_odoo_partner_id AS FLOAT64) AS odoo_partner_id,"
"CAST (property_odoo_user_id AS FLOAT64) AS odoo_user_id,"
"CAST (property_pce_kick_off_date AS TIMESTAMP) AS pce_kick_off_date,"
"CAST (property_rub_personal_car AS BOOL) AS rub_personal_car,"
"CAST (property_single_deal_id AS STRING) AS single_deal_id,"
"CAST (property_vdf_booking_base_price AS FLOAT64) AS vdf_booking_base_price,"
"CAST (property_vdf_booking_deposit AS FLOAT64) AS vdf_booking_deposit,"
"CAST (property_vdf_booking_mileage_package AS STRING) AS vdf_booking_mileage_package,"
"CAST (property_vdf_booking_pickup_station AS STRING) AS vdf_booking_pickup_station,"
"CAST (property_vdf_booking_total_price AS FLOAT64) AS vdf_booking_total_price,"
"CAST (property_vdf_licenseplate AS STRING) AS vdf_licenseplate,"
"CAST (property_vdf_rate_description AS STRING) AS vdf_rate_description,"
"CAST (property_vdf_vehicle_fuel_type AS STRING) AS vdf_vehicle_fuel_type,"
"CAST (property_vdf_vehicle_model AS STRING) AS vdf_vehicle_model,"
"CAST (property_vpc_contract AS STRING) AS vpc_contract,"
"CAST (property_vpc_dua AS STRING) AS vpc_dua,"
"CAST (property_vpc_fueltype AS STRING) AS vpc_fueltype,"
"CAST (property_vpc_ipo AS STRING) AS vpc_ipo,"
"CAST (property_vpc_licenseplate AS STRING) AS vpc_licenseplate,"
"CAST (property_vpc_registrationdate AS TIMESTAMP) AS vpc_registrationdate,"
"CAST (property_vpc_servicefee AS STRING) AS vpc_servicefee,"
"CAST (property_vpc_vehicleactive AS TIMESTAMP) AS vpc_vehicleactive,"
"CAST (property_vpc_vehiclemodel AS STRING) AS vpc_vehiclemodel,"
"CAST (property_vpc_vehicleownership AS STRING) AS vpc_vehicleownership,"
"CAST (property_vpc_vehiclevalid AS TIMESTAMP) AS vpc_vehiclevalid,"
"CAST (property_hs_date_entered_326934765 AS TIMESTAMP) AS hs_date_entered_326934765,"
"CAST (property_hs_date_entered_327026638 AS TIMESTAMP) AS hs_date_entered_327026638,"
"CAST (property_hs_time_in_326934765 AS FLOAT64) AS hs_time_in_326934765,"
"CAST (property_hs_date_entered_327026639 AS TIMESTAMP) AS hs_date_entered_327026639,"
"CAST (property_fen_partner_name AS STRING) AS fen_partner_name,"
"CAST (property_hs_time_in_327026639 AS FLOAT64) AS hs_time_in_327026639,"
"CAST (property_hs_time_in_327026638 AS FLOAT64) AS hs_time_in_327026638,"
"CAST (property_hs_date_exited_327026638 AS TIMESTAMP) AS hs_date_exited_327026638,"
"CAST (property_hs_date_exited_324018669 AS TIMESTAMP) AS hs_date_exited_324018669,"
"CAST (property_hs_time_in_324018670 AS FLOAT64) AS hs_time_in_324018670,"
"CAST (property_hs_date_entered_324018670 AS TIMESTAMP) AS hs_date_entered_324018670,"
"CAST (property_vdf_booking_category AS STRING) AS vdf_booking_category,"
"CAST (property_hs_date_exited_322754803 AS TIMESTAMP) AS hs_date_exited_322754803,"
"CAST (property_hs_date_exited_324018671 AS TIMESTAMP) AS hs_date_exited_324018671,"
"CAST (property_hs_time_in_322754804 AS FLOAT64) AS hs_time_in_322754804,"
"CAST (property_hs_date_exited_324018670 AS TIMESTAMP) AS hs_date_exited_324018670,"
"CAST (property_hs_date_entered_322754804 AS TIMESTAMP) AS hs_date_entered_322754804,"
"CAST (property_hs_date_entered_324018671 AS TIMESTAMP) AS hs_date_entered_324018671,"
"CAST (property_hs_time_in_324018671 AS FLOAT64) AS hs_time_in_324018671,"
"CAST (property_fuel_energy_card_number AS STRING) AS fuel_energy_card_number,"
"CAST (property_ttv_partner_name AS STRING) AS ttv_partner_name,"
"CAST (property_ttv_value AS FLOAT64) AS ttv_value,"
"CAST (property_ttv_start_date AS STRING) AS ttv_start_date,"
"CAST (property_ttv_offer_description AS STRING) AS ttv_offer_description,"
"CAST (property_vdf_booking_pickup_date AS DATETIME) AS vdf_booking_pickup_date,"
"CAST (property_ttv_end_date AS DATETIME) AS ttv_end_date,"
"CAST (property_hs_date_exited_322754804 AS TIMESTAMP) AS hs_date_exited_322754804,"
"CAST (property_hs_time_in_322754806 AS FLOAT64) AS hs_time_in_322754806,"
"CAST (property_hs_date_exited_322754805 AS TIMESTAMP) AS hs_date_exited_322754805,"
"CAST (property_hs_time_in_322754805 AS FLOAT64) AS hs_time_in_322754805,"
"CAST (property_customer_phone_number AS STRING) AS customer_phone_number,"
"CAST (property_hs_date_entered_322754805 AS TIMESTAMP) AS hs_date_entered_322754805,"
"CAST (property_hs_date_entered_322754806 AS TIMESTAMP) AS hs_date_entered_322754806,"
"CAST (property_hs_time_in_327145442 AS FLOAT64) AS hs_time_in_327145442,"
"CAST (property_hs_date_entered_327145442 AS TIMESTAMP) AS hs_date_entered_327145442,"
"CAST (property_hs_date_exited_327026639 AS TIMESTAMP) AS hs_date_exited_327026639,"
"CAST (property_fen_personal_car AS BOOL) AS fen_personal_car,"
"CAST (property_hs_date_entered_326934767 AS TIMESTAMP) AS hs_date_entered_326934767,"
"CAST (property_hs_date_entered_326934768 AS TIMESTAMP) AS hs_date_entered_326934768,"
"CAST (property_hs_time_in_326934768 AS FLOAT64) AS hs_time_in_326934768,"
"CAST (property_hs_time_in_327135477 AS FLOAT64) AS hs_time_in_327135477,"
"CAST (property_hs_date_entered_326934766 AS TIMESTAMP) AS hs_date_entered_326934766,"
"CAST (property_hs_date_exited_326934767 AS TIMESTAMP) AS hs_date_exited_326934767,"
"CAST (property_hs_date_exited_326934765 AS TIMESTAMP) AS hs_date_exited_326934765,"
"CAST (property_hs_time_in_326934767 AS FLOAT64) AS hs_time_in_326934767,"
"CAST (property_hs_date_exited_326934766 AS TIMESTAMP) AS hs_date_exited_326934766,"
"CAST (property_hs_time_in_326934766 AS FLOAT64) AS hs_time_in_326934766,"
"CAST (property_hs_date_entered_327135477 AS TIMESTAMP) AS hs_date_entered_327135477,"
"CAST (property_hs_date_entered_322755007 AS TIMESTAMP) AS hs_date_entered_322755007,"
"CAST (property_hs_date_exited_322810853 AS TIMESTAMP) AS hs_date_exited_322810853,"
"CAST (property_hs_time_in_322755007 AS FLOAT64) AS hs_time_in_322755007,"
"CAST (property_hs_date_exited_327026395 AS TIMESTAMP) AS hs_date_exited_327026395,"
"CAST (property_hs_date_entered_327026398 AS TIMESTAMP) AS hs_date_entered_327026398,"
"CAST (property_hs_time_in_327026398 AS FLOAT64) AS hs_time_in_327026398,"
"CAST (property_hs_date_exited_322754806 AS TIMESTAMP) AS hs_date_exited_322754806,"
"CAST (property_hs_time_in_322810854 AS FLOAT64) AS hs_time_in_322810854,"
"CAST (property_hs_date_entered_322810854 AS TIMESTAMP) AS hs_date_entered_322810854,"
"CAST (property_hs_time_in_322811070 AS FLOAT64) AS hs_time_in_322811070,"
"CAST (property_hs_date_entered_322811070 AS TIMESTAMP) AS hs_date_entered_322811070,"
"CAST (property_hs_date_exited_327145442 AS TIMESTAMP) AS hs_date_exited_327145442,"
"CAST (property_description AS STRING) AS description,"
"CAST (property_hs_tag_ids AS STRING) AS hs_tag_ids,"
"CAST (property_hs_date_exited_326934768 AS TIMESTAMP) AS hs_date_exited_326934768,"
"CAST (property_hs_time_in_307257320 AS FLOAT64) AS hs_time_in_307257320,"
"CAST (property_hs_date_entered_307257320 AS TIMESTAMP) AS hs_date_entered_307257320,"
"CAST (property_hs_time_in_327026643 AS FLOAT64) AS hs_time_in_327026643,"
"CAST (property_grc_md_lost_stage AS STRING) AS grc_md_lost_stage,"
"CAST (property_hs_date_entered_327026643 AS TIMESTAMP) AS hs_date_entered_327026643,"
"CAST (property_lost_reason AS STRING) AS lost_reason,"
"CAST (property_hs_time_in_322675923 AS FLOAT64) AS hs_time_in_322675923,"
"CAST (property_prc_ce_lost_stage AS STRING) AS prc_ce_lost_stage,"
"CAST (property_deal_owner_match AS BOOL) AS deal_owner_match,"
"CAST (property_hs_date_entered_322675923 AS TIMESTAMP) AS hs_date_entered_322675923,"
"CAST (property_deal_owner_compliance AS INT64) AS deal_owner_compliance,"
"CAST (property_hs_time_in_327135476 AS FLOAT64) AS hs_time_in_327135476,"
"CAST (property_hs_date_entered_327135476 AS TIMESTAMP) AS hs_date_entered_327135476,"
"CAST (property_fdl_bf_lost_stage AS STRING) AS fdl_bf_lost_stage,"
"CAST (property_hs_projected_amount AS FLOAT64) AS hs_projected_amount,"
"CAST (property_hs_forecast_amount AS FLOAT64) AS hs_forecast_amount,"
"CAST (property_amount AS FLOAT64) AS amount,"
"CAST (property_hs_projected_amount_in_home_currency AS FLOAT64) AS hs_projected_amount_in_home_currency,"
"CAST (property_amount_in_home_currency AS FLOAT64) AS amount_in_home_currency,"
"CAST (property_rds_ub_lost_stage AS STRING) AS rds_ub_lost_stage,"
"CAST (property_rds_bt_lost_stage AS STRING) AS rds_bt_lost_stage,"
"CAST (property_hs_date_exited_354972637 AS TIMESTAMP) AS hs_date_exited_354972637,"
"CAST (property_hs_time_in_354972637 AS FLOAT64) AS hs_time_in_354972637,"
"CAST (property_hs_date_entered_354972637 AS TIMESTAMP) AS hs_date_entered_354972637,"
"CAST (property_hs_time_in_322754809 AS FLOAT64) AS hs_time_in_322754809,"
"CAST (property_vhc_df_lost_stage AS STRING) AS vhc_df_lost_stage,"
"CAST (property_fdl_ue_lost_stage AS STRING) AS fdl_ue_lost_stage,"
"CAST (property_vhc_pc_lost_stage_clonado_ AS STRING) AS vhc_pc_lost_stage_clonado_,"
"CAST (property_hs_date_entered_322754809 AS TIMESTAMP) AS hs_date_entered_322754809,"
"CAST (property_trn_tv_lost_stage AS STRING) AS trn_tv_lost_stage,"
"CAST (property_ins_vi_lost_stage AS STRING) AS ins_vi_lost_stage,"
"CAST (property_hs_time_in_327026399 AS FLOAT64) AS hs_time_in_327026399,"
"CAST (property_hs_date_entered_327026399 AS TIMESTAMP) AS hs_date_entered_327026399,"
"CAST (property_hs_date_entered_349613800 AS TIMESTAMP) AS hs_date_entered_349613800,"
"CAST (property_hs_time_in_349613800 AS FLOAT64) AS hs_time_in_349613800,"
"CAST (property_fen_pc_lost_stage AS STRING) AS fen_pc_lost_stage,"
"CAST (property_hs_time_in_327026644 AS FLOAT64) AS hs_time_in_327026644,"
"CAST (property_hs_date_entered_327026644 AS TIMESTAMP) AS hs_date_entered_327026644,"
"CAST (property_hs_pinned_engagement_id AS FLOAT64) AS hs_pinned_engagement_id,"
"CAST (property_hs_date_entered_362414286 AS TIMESTAMP) AS hs_date_entered_362414286,"
"CAST (property_hs_time_in_349613805 AS FLOAT64) AS hs_time_in_349613805,"
"CAST (property_hs_date_exited_349613800 AS TIMESTAMP) AS hs_date_exited_349613800,"
"CAST (property_hs_date_exited_349613801 AS TIMESTAMP) AS hs_date_exited_349613801,"
"CAST (property_hs_time_in_349613803 AS FLOAT64) AS hs_time_in_349613803,"
"CAST (property_hs_date_exited_349613802 AS TIMESTAMP) AS hs_date_exited_349613802,"
"CAST (property_hs_time_in_349613802 AS FLOAT64) AS hs_time_in_349613802,"
"CAST (property_hs_date_exited_362414286 AS TIMESTAMP) AS hs_date_exited_362414286,"
"CAST (property_hs_time_in_349613801 AS FLOAT64) AS hs_time_in_349613801,"
"CAST (property_hs_time_in_362414286 AS FLOAT64) AS hs_time_in_362414286,"
"CAST (property_hs_date_exited_349613803 AS TIMESTAMP) AS hs_date_exited_349613803,"
"CAST (property_hs_date_entered_349613801 AS TIMESTAMP) AS hs_date_entered_349613801,"
"CAST (property_hs_date_entered_349613802 AS TIMESTAMP) AS hs_date_entered_349613802,"
"CAST (property_hs_date_entered_349613803 AS TIMESTAMP) AS hs_date_entered_349613803,"
"CAST (property_hs_date_entered_349613805 AS TIMESTAMP) AS hs_date_entered_349613805,"
"CAST (property_fcl_invoice_link AS STRING) AS fcl_invoice_link,"
"CAST (property_deal_owner_vehicles AS INT64) AS deal_owner_vehicles,"
"CAST (property_closed_lost_reason AS STRING) AS closed_lost_reason,"
"CAST (property_points AS FLOAT64) AS points,"
  
  */