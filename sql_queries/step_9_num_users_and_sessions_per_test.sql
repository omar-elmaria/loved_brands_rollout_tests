-- Step 9: Retrieve raw session data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests` AS
WITH raw_sessions_data AS (
  SELECT DISTINCT
    x.created_date, -- Date of the ga session
    ent.region, -- Region
    x.entity_id, -- Entity ID
    x.country_code, -- Country code
    x.platform, -- Operating system (iOS, Android, Web, etc.)
    x.brand, -- Talabat, foodpanda, Foodora, etc.
    x.events_ga_session_id, -- GA session ID
    x.fullvisitor_id, -- The visit_id defined by Google Analytics
    x.visit_id, -- 	The visit_id defined by Google Analytics
    x.has_transaction, -- A field that indicates whether or not a session ended in a transaction
    x.total_transactions, -- The total number of transactions in the GA session
    x.ga_dps_session_id, -- DPS session ID

    x.sessions.dps_session_timestamp, -- The timestamp of the DPS logs
    x.sessions.endpoint, -- The endpoint from where the DPS request is coming
    x.sessions.perseus_client_id, -- A unique customer identifier based on the device
    x.sessions.variant, -- AB variant (e.g. Control, Variation1, Variation2, etc.)
    x.sessions.experiment_id AS test_id, -- Experiment ID
    x.sessions.is_parallel, -- Is this a parallel test?
    LOWER(ven.vertical_parent) AS parent_vertical_vendor_tbl, -- Parent vertical from the vendors table
    CASE 
      WHEN ent.region IN ("Americas", "Asia") THEN
        CASE 
          WHEN LOWER(ven.vertical_parent) = "food" THEN "restaurant"
          WHEN LOWER(ven.vertical_parent) IN ("local store", "dmarts") THEN "shop"
          WHEN LOWER(ven.vertical_parent) = "courier" THEN "courier"
        ELSE LOWER(ven.vertical_parent) END
      WHEN ent.region = "MENA" THEN
        CASE 
          WHEN LOWER(ven.vertical_parent) = "food" THEN "restaurant"
          WHEN LOWER(ven.vertical_parent) = "local store" THEN "shop"
          WHEN LOWER(ven.vertical_parent) = "dmarts" THEN "darkstores"
          WHEN LOWER(ven.vertical_parent) = "courier" THEN "courier"
        ELSE LOWER(ven.vertical_parent) END
      WHEN ent.region = "Europe" THEN LOWER(ven.vertical_parent)
    END AS parent_vertical_test_equivalent,
    vert.first_parent_vertical AS first_parent_vertical_test,
    vert.second_parent_vertical AS second_parent_vertical_test,
    e.vertical_type, -- This field is NULL for event types home_screen.loaded and shop_list.loaded 
    x.sessions.vendor_group_id, -- Target group
    CASE 
        WHEN x.sessions.vendor_group_id IS NULL AND e.vendor_code IS NULL THEN "Unknown"
        WHEN x.sessions.vendor_group_id IS NULL AND e.vendor_code IS NOT NULL THEN "Non Target Group"
        ELSE CONCAT('Target Group ', DENSE_RANK() OVER (PARTITION BY x.entity_id, x.sessions.experiment_id ORDER BY COALESCE(x.sessions.vendor_group_id, 999999)))
    END AS target_group_bi,
    x.sessions.customer_status, -- The customer.tag, indicating whether the customer is new or not
    x.sessions.location, -- The customer.location
    x.sessions.variant_concat, -- The concatenation of all the existing variants for the dps session id. There might be multiple variants due to location changes or session timeout
    x.sessions.location_concat, -- The concatenation of all the existing locations for the dps session id
    x.sessions.customer_status_concat, -- The concatenation of all the existing customer.tag for the dps session id

    e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
    e.vendor_code, -- Vendor ID
    e.event_time, -- The timestamp of the event's creation
    e.transaction_id, -- The transaction id for the GA session if the session has a transaction (i.e. order code)
    e.expedition_type, -- The delivery type of the session, pickup or delivery

    dps.city_id, -- City ID based on the DPS session
    dps.city_name, -- City name based on the DPS session
    dps.id AS zone_id, -- Zone ID based on the DPS session
    dps.name AS zone_name, -- Zone name based on the DPS session
    dps.timezone, -- Time zone of the city based on the DPS session

    ST_ASTEXT(x.ga_location) AS ga_location -- GA location expressed as a STRING
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` AS x
  LEFT JOIN UNNEST(events) AS e
  LEFT JOIN UNNEST(dps_zone) AS dps
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` ven ON x.entity_id = ven.global_entity_id AND e.vendor_code = ven.vendor_id
  LEFT JOIN `dh-logistics-product-ops.pricing.entities_lb_rollout_tests` ent ON x.entity_id = ent.entity_id
  LEFT JOIN (
    SELECT DISTINCT entity_id, test_name, test_id, first_parent_vertical, second_parent_vertical
    FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
  ) vert ON x.entity_id = vert.entity_id AND x.sessions.experiment_id = vert.test_id
  WHERE TRUE
    AND created_date >= DATE("2022-11-28")
    AND CONCAT(x.entity_id, " | ", x.sessions.experiment_id, " | ", x.sessions.variant) IN ( -- Filter for the right variants belonging to the experiment (essentially filter out NULL and Original)
      SELECT DISTINCT CONCAT(entity_id, " | ", test_id, " | ", variant) 
      FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_lb_rollout_tests`
      WHERE CONCAT(entity_id, " | ", test_id, " | ", variant) IS NOT NULL
    )
      
    AND CONCAT(x.entity_id, " | ", x.sessions.experiment_id) IN ( -- Filter for the right entity | experiment_id combos. 
      SELECT DISTINCT CONCAT(entity_id, " | ", test_id)
      FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
      WHERE CONCAT(entity_id, " | ", test_id) IS NOT NULL
    )
)

SELECT 
  a.*,
  CASE
    WHEN event_action IN ("home_screen.loaded", "shop_list.loaded") THEN "Unknown"
    WHEN event_action NOT IN ("home_screen.loaded", "shop_list.loaded") AND target_group_bi IN ("Non Target Group", "Unknown") THEN "N"
    WHEN event_action NOT IN ("home_screen.loaded", "shop_list.loaded") AND target_group_bi NOT IN ("Non Target Group", "Unknown") THEN "Y"
    ELSE NULL
  END AS is_session_in_treatment_raw
FROM raw_sessions_data
;