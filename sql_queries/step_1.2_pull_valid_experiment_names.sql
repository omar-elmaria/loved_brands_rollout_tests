-- Step 1.2: Pull the valid experiment names
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests` AS 
SELECT DISTINCT
  entity_id,
  country_code,
  test_id,
  test_name,
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
WHERE TRUE
  AND DATE(test_start_date) >= DATE("2022-11-28") -- Filter for tests that started from November 28th, 2022 (date of the first Loved Brands test using the productionized pipeline)
  AND (LOWER(test_name) LIKE "%loved_brands%" OR LOWER(test_name) LIKE "%love_brands%" OR LOWER(test_name) LIKE "%lb%" OR LOWER(test_name) LIKE "%lovedbrands%" OR LOWER(test_name) LIKE "%lovebrands%")
;
