SELECT
  MIN(transaction_date) as earliest_txn,
  MAX(transaction_date) as latest_txn,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(*) as total_txns,
  ROUND(AVG(amount), 2) as avg_amount,
  ROUND(APPROX_QUANTILES(amount, 100)[OFFSET(50)], 2) as median_amount,
  ROUND(MIN(amount), 2) as min_amount,
  ROUND(MAX(amount), 2) as max_amount,
  ROUND(COUNTIF(channel = 'online') / COUNT(*) * 100, 1) as online_pct,
  ROUND(COUNTIF(promo_flag = TRUE) / COUNT(*) * 100, 1) as promo_pct,
  ROUND(AVG(discount_pct) * 100, 1) as avg_discount_when_promo
FROM `hd-segmentation-sim.hd_segmentation.transactions`;
