SELECT
  s.old_segment_id,
  s.segment_name,
  COUNT(DISTINCT t.transaction_id) as total_txns,
  COUNT(DISTINCT t.customer_id) as customers,
  ROUND(AVG(t.amount), 2) as avg_basket,
  ROUND(APPROX_QUANTILES(t.amount, 100)[OFFSET(50)], 2) as median_basket,
  ROUND(COUNT(DISTINCT t.transaction_id) / COUNT(DISTINCT t.customer_id), 1) as avg_txns_per_cust,
  ROUND(COUNTIF(t.channel = 'online') / COUNT(*) * 100, 1) as online_pct,
  ROUND(COUNTIF(t.promo_flag = TRUE) / COUNT(*) * 100, 1) as promo_pct,
  COUNT(DISTINCT t.category) as distinct_categories
FROM `hd-segmentation-sim.hd_segmentation.old_segments` s
JOIN `hd-segmentation-sim.hd_segmentation.transactions` t
  ON s.customer_id = t.customer_id
GROUP BY s.old_segment_id, s.segment_name
ORDER BY s.old_segment_id;
