SELECT
  s.old_segment_id,
  s.segment_name,
  COUNT(*) as campaigns_sent,
  ROUND(AVG(c.opened) * 100, 1) as open_rate,
  ROUND(AVG(c.clicked) * 100, 1) as click_rate,
  ROUND(AVG(c.converted) * 100, 1) as conversion_rate,
  ROUND(SUM(c.revenue) / COUNT(*), 2) as revenue_per_send
FROM `hd-segmentation-sim.hd_segmentation.campaigns` c
JOIN `hd-segmentation-sim.hd_segmentation.old_segments` s
  ON c.customer_id = s.customer_id
GROUP BY s.old_segment_id, s.segment_name
ORDER BY s.old_segment_id;
