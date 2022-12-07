-- Step 9: Append new rows to the final table that is used in the dashboard and Py script (`dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_lb_rollout_tests`)
INSERT `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_lb_rollout_tests`
SELECT *
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_staging_lb_rollout_tests`
WHERE created_date_utc > (SELECT MAX(created_date_utc) FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_lb_rollout_tests`) -- SELECT ALL records from the staging table that have a created_date > MAX(created_date) in the table used in the dashboard and Py script