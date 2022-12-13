-- Step 1.1: Create a table containing the region of each entity ID
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.entities_lb_rollout_tests` AS
SELECT
    ent.region,
    p.entity_id,
    ent.country_iso,
    ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE "ODR%" -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE "DN_%" -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ("FP_DE", "FP_JP") -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != "TB_SA" -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != "HS_BH" -- Eliminate this incorrect entity_id for Bahrain
;