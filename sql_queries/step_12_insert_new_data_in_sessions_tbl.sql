-- Step 12: Insert new data in the sessions table
INSERT `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests`
SELECT *
FROM `dh-logistics-product-ops.pricing.ga_sessions_data_stg_lb_rollout_tests`
WHERE created_date > (SELECT MAX(created_date) FROM `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests`); -- SELECT ALL records from the staging table that have a created_date > MAX(created_date) in the table used in the dashboard and Py script