CREATE OR REPLACE TABLE `hd-segmentation-sim.hd_segmentation.customer_features` AS

WITH reference AS (
  SELECT MAX(transaction_date) AS ref_date
  FROM `hd-segmentation-sim.hd_segmentation.transactions`
),

window_config AS (
  SELECT
    ref_date,
    CASE
      WHEN EXTRACT(QUARTER FROM ref_date) IN (1, 3) THEN 84
      ELSE 56
    END AS window_days
  FROM reference
),

eligible_customers AS (
  SELECT customer_id, COUNT(*) AS total_txn_count
  FROM `hd-segmentation-sim.hd_segmentation.transactions`
  GROUP BY customer_id
  HAVING COUNT(*) >= 3
),

behavioral AS (
  SELECT
    t.customer_id,
    COUNTIF(t.transaction_date >= DATE_SUB(w.ref_date, INTERVAL w.window_days DAY))
      AS txns_in_window,
    ROUND(
      COUNTIF(t.transaction_date >= DATE_SUB(w.ref_date, INTERVAL w.window_days DAY))
      / (w.window_days / 7.0), 2
    ) AS weekly_purchase_frequency,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT t.category) AS category_breadth,
    ROUND(AVG(t.amount), 2) AS avg_basket_size,
    ROUND(APPROX_QUANTILES(t.amount, 100)[OFFSET(50)], 2) AS median_basket_size,
    ROUND(COUNTIF(t.channel = 'online') / COUNT(*), 4) AS online_ratio,
    COUNTIF(t.category IN ('Lumber', 'Flooring', 'Kitchen & Bath', 'Plumbing', 'Appliances'))
      AS project_category_txns,
    COUNTIF(t.category IN ('Hardware', 'Paint', 'Lighting', 'Electrical', 'Garden & Outdoor'))
      AS maintenance_category_txns
  FROM `hd-segmentation-sim.hd_segmentation.transactions` t
  CROSS JOIN window_config w
  WHERE t.customer_id IN (SELECT customer_id FROM eligible_customers)
  GROUP BY t.customer_id, w.ref_date, w.window_days
),

project_detection AS (
  SELECT
    t.customer_id,
    COUNT(DISTINCT
      CASE WHEN t.category IN ('Lumber', 'Flooring', 'Kitchen & Bath', 'Plumbing', 'Appliances')
           AND t.transaction_date >= DATE_SUB(w.ref_date, INTERVAL w.window_days DAY)
      THEN t.category END
    ) AS project_categories_in_window
  FROM `hd-segmentation-sim.hd_segmentation.transactions` t
  CROSS JOIN window_config w
  WHERE t.customer_id IN (SELECT customer_id FROM eligible_customers)
  GROUP BY t.customer_id
),

price_sensitivity AS (
  SELECT
    t.customer_id,
    ROUND(COUNTIF(t.promo_flag = TRUE) / COUNT(*), 4) AS promo_purchase_ratio,
    ROUND(
      SAFE_DIVIDE(
        SUM(CASE WHEN t.promo_flag = TRUE THEN t.discount_pct ELSE 0 END),
        COUNTIF(t.promo_flag = TRUE)
      ), 4
    ) AS avg_discount_depth,
    ROUND(COUNTIF(t.promo_flag = FALSE) / COUNT(*), 4) AS full_price_ratio
  FROM `hd-segmentation-sim.hd_segmentation.transactions` t
  WHERE t.customer_id IN (SELECT customer_id FROM eligible_customers)
  GROUP BY t.customer_id
),

time_aware AS (
  SELECT
    t.customer_id,
    DATE_DIFF(w.ref_date, MAX(t.transaction_date), DAY) AS days_since_last_purchase,
    ROUND(
      COUNT(*) / GREATEST(DATE_DIFF(MAX(t.transaction_date), MIN(t.transaction_date), DAY) / 30.0, 1),
      2
    ) AS monthly_frequency,
    ROUND(
      SAFE_DIVIDE(
        COUNTIF(t.transaction_date >= DATE_ADD(
          MIN(t.transaction_date),
          INTERVAL CAST(DATE_DIFF(MAX(t.transaction_date), MIN(t.transaction_date), DAY) / 2 AS INT64) DAY
        )),
        COUNTIF(t.transaction_date < DATE_ADD(
          MIN(t.transaction_date),
          INTERVAL CAST(DATE_DIFF(MAX(t.transaction_date), MIN(t.transaction_date), DAY) / 2 AS INT64) DAY
        ))
      ) - 1.0,
      4
    ) AS frequency_trend,
    ROUND(
      COUNTIF(EXTRACT(QUARTER FROM t.transaction_date) = 2) / COUNT(*),
      4
    ) AS q2_concentration,
    ROUND(
      COUNTIF(EXTRACT(DAYOFWEEK FROM t.transaction_date) IN (1, 7)) / COUNT(*),
      4
    ) AS weekend_ratio
  FROM `hd-segmentation-sim.hd_segmentation.transactions` t
  CROSS JOIN window_config w
  WHERE t.customer_id IN (SELECT customer_id FROM eligible_customers)
  GROUP BY t.customer_id, w.ref_date
),

session_stats AS (
  SELECT
    session_id,
    customer_id,
    COUNT(*) AS page_count
  FROM `hd-segmentation-sim.hd_segmentation.web_events`
  WHERE customer_id IN (SELECT customer_id FROM eligible_customers)
  GROUP BY session_id, customer_id
),

web_behavior AS (
  SELECT
    ss.customer_id,
    COUNT(DISTINCT ss.session_id) AS total_sessions,
    ROUND(SUM(ss.page_count) / COUNT(DISTINCT ss.session_id), 2) AS avg_pages_per_session,
    ROUND(COUNTIF(ss.page_count >= 5) / COUNT(DISTINCT ss.session_id), 4) AS deep_browse_ratio
  FROM session_stats ss
  GROUP BY ss.customer_id
),

web_page_stats AS (
  SELECT
    we.customer_id,
    ROUND(SUM(we.search_flag) / GREATEST(COUNT(DISTINCT we.session_id), 1), 4) AS search_rate,
    ROUND(COUNTIF(we.page_type = 'product') / COUNT(*), 4) AS product_page_ratio,
    ROUND(COUNTIF(we.page_type = 'cart') / COUNT(*), 4) AS cart_page_ratio,
    ROUND(COUNTIF(we.device = 'mobile') / COUNT(*), 4) AS mobile_ratio
  FROM `hd-segmentation-sim.hd_segmentation.web_events` we
  WHERE we.customer_id IN (SELECT customer_id FROM eligible_customers)
  GROUP BY we.customer_id
),

loyalty_features AS (
  SELECT
    l.customer_id,
    CASE l.loyalty_tier
      WHEN 'Gold' THEN 3
      WHEN 'Silver' THEN 2
      WHEN 'Bronze' THEN 1
      ELSE 0
    END AS loyalty_tier_numeric,
    l.points_balance
  FROM `hd-segmentation-sim.hd_segmentation.loyalty` l
  WHERE l.customer_id IN (SELECT customer_id FROM eligible_customers)
),

regional AS (
  SELECT
    cm.customer_id,
    cm.region
  FROM `hd-segmentation-sim.hd_segmentation.customer_master` cm
  WHERE cm.customer_id IN (SELECT customer_id FROM eligible_customers)
)

SELECT
  b.customer_id,
  r.region,
  b.total_transactions,
  b.txns_in_window,
  b.weekly_purchase_frequency,
  b.category_breadth,
  b.avg_basket_size,
  b.median_basket_size,
  b.online_ratio,
  b.project_category_txns,
  b.maintenance_category_txns,
  ROUND(SAFE_DIVIDE(b.project_category_txns, b.total_transactions), 4) AS project_ratio,
  CASE WHEN pd.project_categories_in_window >= 2 THEN 1 ELSE 0 END AS is_project_buyer,
  ps.promo_purchase_ratio,
  ps.avg_discount_depth,
  ps.full_price_ratio,
  ta.days_since_last_purchase,
  ta.monthly_frequency,
  ta.frequency_trend,
  ta.q2_concentration,
  ta.weekend_ratio,
  COALESCE(wb.total_sessions, 0) AS total_sessions,
  COALESCE(wb.avg_pages_per_session, 0) AS avg_pages_per_session,
  COALESCE(wps.search_rate, 0) AS search_rate,
  COALESCE(wps.product_page_ratio, 0) AS product_page_ratio,
  COALESCE(wps.cart_page_ratio, 0) AS cart_page_ratio,
  COALESCE(wps.mobile_ratio, 0) AS mobile_ratio,
  COALESCE(wb.deep_browse_ratio, 0) AS deep_browse_ratio,
  COALESCE(lf.loyalty_tier_numeric, 0) AS loyalty_tier_numeric,
  COALESCE(lf.points_balance, 0) AS points_balance
FROM behavioral b
JOIN project_detection pd ON b.customer_id = pd.customer_id
JOIN price_sensitivity ps ON b.customer_id = ps.customer_id
JOIN time_aware ta ON b.customer_id = ta.customer_id
LEFT JOIN web_behavior wb ON b.customer_id = wb.customer_id
LEFT JOIN web_page_stats wps ON b.customer_id = wps.customer_id
LEFT JOIN loyalty_features lf ON b.customer_id = lf.customer_id
JOIN regional r ON b.customer_id = r.customer_id
ORDER BY b.customer_id;
