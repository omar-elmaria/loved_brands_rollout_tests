-- Step 11: Join the treatment flag to the sessions data and add the test_name
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests` AS
SELECT
  a.*,
  b.is_session_in_treatment_agg,
  tst.test_name
FROM `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests` a
LEFT JOIN `dh-logistics-product-ops.pricing.treatment_flag_per_session_lb_rollout_tests` b ON a.entity_id = b.entity_id AND a.test_id = b.test_id AND a.events_ga_session_id = b.events_ga_session_id
LEFT JOIN `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests` tst ON a.entity_id = tst.entity_id AND a.test_id = tst.test_id
;
