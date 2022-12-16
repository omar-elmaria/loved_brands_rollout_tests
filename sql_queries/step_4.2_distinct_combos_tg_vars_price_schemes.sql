-- Step 4.2: Find the distinct combinations of target groups, variants, and price schemes per test
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_agg_tgs_variants_and_schemes_lb_rollout_tests` AS
SELECT 
  entity_id,
  country_code,
  test_name,
  test_id,
  ARRAY_TO_STRING(ARRAY_AGG(CONCAT(target_group, " | ", variant, " | ", price_scheme_id)), ", ") AS tg_var_scheme_concat
FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_lb_rollout_tests`
GROUP BY 1,2,3,4
;
