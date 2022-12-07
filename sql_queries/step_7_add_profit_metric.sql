-- Step 7: We did not add the profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_lb_rollout_tests` AS
SELECT
  a.*,
  -- Revenue and profit formulas
  actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local, sof_local_cdwh) AS revenue_local,
  actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local, sof_local_cdwh) - delivery_costs_local AS gross_profit_local,

FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_lb_rollout_tests` a
WHERE TRUE -- Filter for orders from the right parent vertical (restuarants, shop, darkstores, etc.) per experiment
    AND (
      CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", first_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
        WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", first_parent_vertical) IS NOT NULL
      )
      OR
      CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", second_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
        WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", second_parent_vertical) IS NOT NULL
      )
    );