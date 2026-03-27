SELECT 'customer_master' as table_name, COUNT(*) as row_count FROM `hd-segmentation-sim.hd_segmentation.customer_master`
UNION ALL
SELECT 'transactions', COUNT(*) FROM `hd-segmentation-sim.hd_segmentation.transactions`
UNION ALL
SELECT 'web_events', COUNT(*) FROM `hd-segmentation-sim.hd_segmentation.web_events`
UNION ALL
SELECT 'loyalty', COUNT(*) FROM `hd-segmentation-sim.hd_segmentation.loyalty`
UNION ALL
SELECT 'old_segments', COUNT(*) FROM `hd-segmentation-sim.hd_segmentation.old_segments`
UNION ALL
SELECT 'campaigns', COUNT(*) FROM `hd-segmentation-sim.hd_segmentation.campaigns`
ORDER BY table_name;
