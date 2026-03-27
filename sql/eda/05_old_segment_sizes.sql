SELECT
  s.old_segment_id,
  s.segment_name,
  COUNT(*) as customer_count,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 1) as pct_of_total,
  s.assignment_date
FROM `hd-segmentation-sim.hd_segmentation.old_segments` s
GROUP BY s.old_segment_id, s.segment_name, s.assignment_date
ORDER BY s.old_segment_id;
