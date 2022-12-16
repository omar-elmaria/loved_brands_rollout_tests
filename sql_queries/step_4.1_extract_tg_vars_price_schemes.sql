-- Step 4.1: Extract the target groups, variants, and price schemes of the tests
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_lb_rollout_tests` AS
SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    CONCAT("TG", priority) AS target_group,
    variation_group AS variant,
    price_scheme_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE 
  AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests`)
ORDER BY 1,2
;
