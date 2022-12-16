-- Step 10: Create a treatment flag per session
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.treatment_flag_per_session_lb_rollout_tests` AS
WITH agg_session_stats AS (
  SELECT
    entity_id,
    test_id,
    events_ga_session_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT event_action ORDER BY event_action), ", ") AS distinct_event_actions_per_session,
    SUM(CASE WHEN is_session_in_treatment_raw = "Y" THEN 1 ELSE 0 END) AS num_instances_with_treated_vendors,
    SUM(CASE WHEN is_session_in_treatment_raw = "N" THEN 1 ELSE 0 END) AS num_instances_with_no_treated_vendors,
    SUM(CASE WHEN is_session_in_treatment_raw = "Unknown" THEN 1 ELSE 0 END) AS num_instances_with_unknown_treatment_vendors,
  FROM `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests`
  GROUP BY 1,2,3
)

SELECT
  *,
  CASE
    WHEN 
      distinct_event_actions_per_session = "home_screen.loaded" 
      OR distinct_event_actions_per_session = "home_screen.loaded, shop_list.loaded"
      OR distinct_event_actions_per_session = "shop_list.loaded" THEN "Unknown"
    WHEN 
      distinct_event_actions_per_session != "home_screen.loaded"
      AND distinct_event_actions_per_session != "home_screen.loaded, shop_list.loaded"
      AND distinct_event_actions_per_session != "shop_list.loaded" 
      AND num_instances_with_treated_vendors >= 1 THEN "Y"
  ELSE "N"
  END AS is_session_in_treatment_agg
FROM agg_session_stats
;