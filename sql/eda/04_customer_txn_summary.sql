SELECT
  ROUND(AVG(txn_count), 1) as avg_txns_per_customer,
  ROUND(APPROX_QUANTILES(txn_count, 100)[OFFSET(50)], 1) as median_txns,
  MIN(txn_count) as min_txns,
  MAX(txn_count) as max_txns,
  COUNTIF(txn_count < 3) as customers_below_3_txns,
  COUNTIF(txn_count >= 30) as power_users
FROM (
  SELECT customer_id, COUNT(*) as txn_count
  FROM `hd-segmentation-sim.hd_segmentation.transactions`
  GROUP BY customer_id
);
