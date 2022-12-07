-- Step 8: Clean the orders data by filtering for records where keep_drop_flag = "Keep" (refer to the code above to see how this field was constructed)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_staging_lb_rollout_tests` AS
SELECT
    *
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_lb_rollout_tests`
WHERE TRUE
    AND keep_drop_flag = "Keep"; -- Filter for the orders that have the correct target_group, variant, and scheme ID based on the configuration of the experiment