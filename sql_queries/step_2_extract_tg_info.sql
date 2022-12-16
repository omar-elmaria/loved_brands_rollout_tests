-- Step 2: Extract the vendor IDs per target group along with their associated parent vertical and vertical type
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests` AS
WITH vendor_tg_vertical_mapping_with_dup AS (
  SELECT DISTINCT -- The DISTINCT command is important here
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    vendor_group_id,
    vendor_id AS vendor_code,
    parent_vertical, -- The parent vertical can only assume 7 values "Restaurant", "Shop", "darkstores", "restaurant", "restaurants", "shop", or NULL. The differences are due platform configurations
    CONCAT("TG", DENSE_RANK() OVER (PARTITION BY entity_id, test_name ORDER BY vendor_group_id)) AS tg_name,
    
    -- Time condition parameters
    schedule.id AS tc_id,
    schedule.priority AS tc_priority,
    schedule.start_at,
    schedule.recurrence_end_at,
    active_days,

    -- Customer condition parameters
    customer_condition.id AS cc_id,
    customer_condition.priority AS cc_priority,
    customer_condition.orders_number_less_than,
    customer_condition.days_since_first_order_less_than,
  FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
  CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
  LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
  LEFT JOIN UNNEST(schedule.active_days) active_days
  WHERE TRUE 
    AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests`)
),

vendor_tg_vertical_mapping_agg AS (
  SELECT 
    * EXCEPT (parent_vertical),
    ARRAY_TO_STRING(ARRAY_AGG(parent_vertical RESPECT NULLS ORDER BY parent_vertical), ", ") AS parent_vertical_concat -- We do this step because some tests have two parent verticals. If we do not aggregate, we will get duplicates 
  FROM vendor_tg_vertical_mapping_with_dup 
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

SELECT
  a.*,
  CASE 
    WHEN parent_vertical_concat = "" THEN NULL -- Case 1
    WHEN parent_vertical_concat LIKE "%,%" THEN -- Case 2 (tests where multiple parent verticals were chosen during configuration)
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r"(.*),\s") IN ("restaurant", "restaurants") THEN "restaurant"
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r"(.*),\s") = "shop" THEN "shop"
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r"(.*),\s") = "darkstores" THEN "darkstores"
      END
    -- Case 3 (tests where a single parent vertical was chosen during configuration)
    WHEN LOWER(parent_vertical_concat) IN ("restaurant", "restaurants") THEN "restaurant"
    WHEN LOWER(parent_vertical_concat) = "shop" THEN "shop"
    WHEN LOWER(parent_vertical_concat) = "darkstores" THEN "darkstores"
  ELSE REGEXP_SUBSTR(parent_vertical_concat, r"(.*),\s") END AS first_parent_vertical,
  
  CASE
    WHEN parent_vertical_concat = "" THEN NULL
    WHEN parent_vertical_concat LIKE "%,%" THEN
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r",\s(.*)") IN ("restaurant", "restaurants") THEN "restaurant"
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r",\s(.*)") = "shop" THEN "shop"
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r",\s(.*)") = "darkstores" THEN "darkstores"
      END
  END AS second_parent_vertical,
  b.vertical_type -- Vertical type of the vendor (NOT parent vertical)
FROM vendor_tg_vertical_mapping_agg a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` b ON a.entity_id = b.global_entity_id AND a.vendor_code = b.vendor_id
ORDER BY 1,2,3,4,5
;
