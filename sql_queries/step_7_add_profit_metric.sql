-- Step 7: We did not add the profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_lb_rollout_tests` AS
SELECT
  a.*,
  -- Revenue and profit formulas
  SAFE_DIVIDE(a.actual_df_paid_by_customer, 1 + b.vat_ratio) + a.commission_local + a.joker_vendor_fee_local + SAFE_DIVIDE(a.service_fee_local, 1 + b.vat_ratio) + SAFE_DIVIDE(COALESCE(a.sof_local, a.sof_local_cdwh), 1 + b.vat_ratio) AS revenue_local,
  SAFE_DIVIDE(a.actual_df_paid_by_customer, 1 + b.vat_ratio) + a.commission_local + a.joker_vendor_fee_local + SAFE_DIVIDE(a.service_fee_local, 1 + b.vat_ratio) + SAFE_DIVIDE(COALESCE(a.sof_local, a.sof_local_cdwh), 1 + b.vat_ratio) - a.delivery_costs_local AS gross_profit_local,

FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_lb_rollout_tests` a
INNER JOIN `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` b ON a.entity_id = b.entity_id AND a.platform_order_code = b.platform_order_code
WHERE TRUE -- Filter for orders from the right parent vertical (restuarants, shop, darkstores, etc.) per experiment
    AND (
      CONCAT(a.entity_id, " | ", a.country_code, " | ", a.test_id, " | ", a.vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", first_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
        WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", first_parent_vertical) IS NOT NULL
      )
      OR
      CONCAT(a.entity_id, " | ", a.country_code, " | ", a.test_id, " | ", a.vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", second_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
        WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", second_parent_vertical) IS NOT NULL
      )
    )
;