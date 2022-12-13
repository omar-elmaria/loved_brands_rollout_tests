-- Step 11: Join the treatment flag to the sessions data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests` AS
SELECT a.*, b.is_session_in_treatment_agg
FROM `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests` a
LEFT JOIN `dh-logistics-product-ops.pricing.treatment_flag_per_session_lb_rollout_tests` b USING(entity_id, test_id, events_ga_session_id)
;
