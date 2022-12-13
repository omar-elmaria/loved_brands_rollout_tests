-- Step 6: Pull the business KPIs from dps_sessions_mapped_to_orders_v2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_lb_rollout_tests` AS
WITH test_start_and_end_dates AS ( -- Get the start and end dates per test
  SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id
  FROM `dh-logistics-product-ops.pricing.ab_test_zone_ids_lb_rollout_tests`
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date AS created_date_utc,
    a.order_placed_at AS order_placed_at_utc,
    a.order_placed_at_local,
    FORMAT_DATE("%A", DATE(order_placed_at_local)) AS dow_local,
    a.dps_sessionid_created_at AS dps_sessionid_created_at_utc,
    DATE_DIFF(DATE(a.order_placed_at_local), DATE_ADD(DATE(dat.test_start_date), INTERVAL 1 DAY), DAY) + 1 AS day_num_in_test, -- We add "+1" so that the first day gets a "1" not a "0"

    -- Location of order
    a.region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,
    zn.zone_shape,
    ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude) AS customer_location,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id AS test_id,
    dat.test_name,
    dat.test_start_date,
    dat.test_end_date,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.customer_total_orders,
    a.customer_first_order_date,
    DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) AS days_since_first_order,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as "Automatic", "Manual", "Campaign", and "Country Fallback".
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    COALESCE(tg.tg_name, "Non_TG") AS target_group,
    b.target_group AS target_group_bi,
    a.is_in_treatment,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    CASE 
      WHEN a.vendor_vertical_parent IS NULL THEN NULL 
      WHEN LOWER(a.vendor_vertical_parent) IN ("restaurant", "restaurants") THEN "restaurant"
      WHEN LOWER(a.vendor_vertical_parent) = "shop" THEN "shop"
      WHEN LOWER(a.vendor_vertical_parent) = "darkstores" THEN "darkstores"
    END AS vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (These are the components of profit)
    a.dps_delivery_fee_local,
    a.delivery_fee_local,
    a.dps_travel_time_fee_local,
    a.commission_local,
    a.joker_vendor_fee_local,
    COALESCE(a.service_fee_local, 0) AS service_fee_local,
    dwh.value.mov_customer_fee_local AS sof_local_cdwh,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    a.delivery_costs_local,
    CASE
        WHEN ent.region IN ("Europe", "Asia") THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            -- In 99 pct of cases, we won"t need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ("Europe", "Asia") THEN (CASE WHEN a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE a.delivery_fee_local END)
    END AS actual_df_paid_by_customer,
    a.gfv_local,
    a.gmv_local,

    -- Logistics KPIs
    a.mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order at session start time (Used by dashboard, das, dps). This data point is only available for OD orders
    a.dps_mean_delay, -- A.K.A DPS Average fleet delay --> Average lateness in minutes of an order placed at this time coming from DPS service
    a.dps_mean_delay_zone_id, -- ID of the zone where fleet delay applies
    a.travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    a.dps_travel_time, -- The calculated travel time in minutes from the vendor to customer coming from DPS
    a.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders
    a.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates
    -- This distance doesn"t take into account potential stacked deliveries, and it"s not the travelled distance. This data point is only available for OD orders.
    a.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    a.actual_DT, -- The time it took to deliver the order. Measured from order creation until rider at customer. This data point is only available for OD orders.

    -- Special fields
    a.is_delivery_fee_covered_by_discount, -- Needed in the profit formula
    a.is_delivery_fee_covered_by_voucher, -- Needed in the profit formula
    tg.parent_vertical_concat,
    -- This filter is used to clean the data. It removes all orders that did not belong to the correct target_group, variant, scheme_id combination as dictated by the experiment"s setup
    CASE WHEN COALESCE(tg.tg_name, "Non_TG") = "Non_TG" OR vs.tg_var_scheme_concat LIKE CONCAT("%", COALESCE(tg.tg_name, "Non_TG"), " | ", a.variant, " | ", a.scheme_id, "%") THEN "Keep" ELSE "Drop" END AS keep_drop_flag,
    CASE WHEN 
      COALESCE(b.target_group, "Non_TG") = "Non_TG"
      OR
      vs.tg_var_scheme_concat LIKE CONCAT("%", COALESCE(CONCAT("TG", REGEXP_EXTRACT(b.target_group, r'\d+')), "Non_TG"), " | ", a.variant, " | ", a.scheme_id, "%") THEN "Keep" 
    ELSE "Drop"
    END AS keep_drop_flag_bi
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` b ON a.entity_id = b.entity_id AND a.order_id = b.order_id
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
  ON TRUE 
    AND a.entity_id = dwh.global_entity_id
    AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
  ON TRUE 
    AND a.entity_id = pd.global_entity_id
    AND a.platform_order_code = pd.code 
    AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_lb_rollout_tests` zn 
  ON TRUE 
    AND a.entity_id = zn.entity_id 
    AND a.country_code = zn.country_code
    AND a.zone_id = zn.zone_id 
    AND a.experiment_id = zn.test_id -- Filter for orders in the target zones (combine this JOIN with the condition in the WHERE clause)
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests` tg -- Tag the vendors with their target group association
  ON TRUE
    AND a.entity_id = tg.entity_id
    AND a.vendor_id = tg.vendor_code 
    AND a.experiment_id = tg.test_id 
    AND
      CASE WHEN DATE(tg.start_at) IS NULL AND DATE(tg.recurrence_end_at) IS NULL AND tg.active_days IS NULL THEN TRUE -- If there is no time condition in the experiment, skip the join step
      ELSE -- If there is, assign orders to the relevant target groups depending on the two time condition parts
        DATE(a.order_placed_at_local) BETWEEN DATE(tg.start_at) AND DATE(tg.recurrence_end_at) -- A join for the time condition (1)
        AND UPPER(FORMAT_DATE("%A", DATE(a.order_placed_at_local))) = tg.active_days -- A join for the time condition (2)
      END
    AND 
      CASE WHEN tg.orders_number_less_than IS NULL AND tg.days_since_first_order_less_than IS NULL THEN TRUE -- If there is no customer condition in the experiment, skip the join step
      ELSE -- If there is, assign the orders with dps_customer_tag = "New" to their relevant target groups depending on the "calendar week" AND the two customer condition parameters (total_orders and days_since_first_order)
        a.customer_total_orders < tg.orders_number_less_than -- customer_total_orders always > 0 when dps_customer_tag = "New"
        -- customer_first_order_date could be NULL or have a DATETIME value. In both cases, dps_customer_tag could be equal to "New"
        AND (DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) < tg.days_since_first_order_less_than OR DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) IS NULL)
      END
LEFT JOIN test_start_and_end_dates dat ON a.entity_id = dat.entity_id AND a.country_code = dat.country_code AND a.experiment_id = dat.test_id
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_agg_tgs_variants_and_schemes_lb_rollout_tests` vs -- Get the list of target_group | variation | scheme_id combinations that are relevant to the experiment
  ON TRUE
    AND a.entity_id = vs.entity_id 
    AND a.country_code = vs.country_code 
    AND a.experiment_id = vs.test_id
INNER JOIN `dh-logistics-product-ops.pricing.entities_lb_rollout_tests` ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
    AND a.created_date >= DATE("2022-11-28") -- Filter for tests that started from November 28th, 2022 (date of the first Loved Brands test using the productionized pipeline)
    
    AND CONCAT(a.entity_id, " | ", a.country_code, " | ", a.experiment_id, " | ", a.variant) IN ( -- Filter for the right variants belonging to the experiment (essentially filter out NULL and Original)
      SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", variant) 
      FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_lb_rollout_tests`
      WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", variant) IS NOT NULL
    )
    
    AND a.delivery_status = "completed" -- Successful orders
    
    AND CONCAT(a.entity_id, " | ", a.country_code, " | ", a.experiment_id) IN ( -- Filter for the right entity | experiment_id combos. 
      -- The "ab_test_target_groups_lb_rollout_tests" table was specifically chosen from the tables in steps 2-4 because it automatically eliminates tests where there are no matching vendors
      SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id)
      FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
      WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id) IS NOT NULL
    )
    
    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)) -- Filter for orders coming from the target zones
;