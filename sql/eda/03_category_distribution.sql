SELECT
  category,
  COUNT(*) as txn_count,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 1) as pct_of_txns,
  ROUND(AVG(amount), 2) as avg_amount,
  COUNT(DISTINCT customer_id) as unique_customers
FROM `hd-segmentation-sim.hd_segmentation.transactions`
GROUP BY category
ORDER BY txn_count DESC;
