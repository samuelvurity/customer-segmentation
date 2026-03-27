"""
Export data from BigQuery to CSV for Tableau Public.

Tableau Public can't connect to BigQuery directly.
This script exports pre-shaped datasets optimized for each dashboard view.

Views:
  1. Segment Health Overview
  2. Segment Migration
  3. Campaign Performance by Segment
  4. Segment Behavioral Profiles
  5. Churn Indicators by Segment
"""

import pandas as pd
from google.cloud import bigquery
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.project_config import GCP_PROJECT_ID, BQ_DATASET, TABLES

EXPORT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'tableau_export')


def export_segment_health(client):
    """View 1: Segment sizes, revenue, AOV, frequency."""

    query = f"""
    SELECT
      sa.segment_id,
      sa.segment_name,
      COUNT(DISTINCT sa.customer_id) as customer_count,
      ROUND(COUNT(DISTINCT sa.customer_id) / SUM(COUNT(DISTINCT sa.customer_id)) OVER() * 100, 1) as pct_of_total,
      ROUND(SUM(t.amount), 2) as total_revenue,
      ROUND(AVG(t.amount), 2) as avg_order_value,
      ROUND(SUM(t.amount) / COUNT(DISTINCT sa.customer_id), 2) as revenue_per_customer,
      ROUND(COUNT(t.transaction_id) / COUNT(DISTINCT sa.customer_id), 1) as avg_txns_per_customer
    FROM `{TABLES['segment_assignments']}` sa
    JOIN `{TABLES['transactions']}` t ON sa.customer_id = t.customer_id
    GROUP BY sa.segment_id, sa.segment_name
    ORDER BY sa.segment_id
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '01_segment_health.csv'), index=False)
    print(f"  segment_health: {len(df)} rows")
    return df


def export_segment_health_over_time(client):
    """View 1 continued: segment sizes by quarter (simulated weekly refresh trend)."""

    query = f"""
    SELECT
      sa.segment_id,
      sa.segment_name,
      EXTRACT(YEAR FROM t.transaction_date) as year,
      EXTRACT(QUARTER FROM t.transaction_date) as quarter,
      FORMAT_DATE('%Y-Q%%s', t.transaction_date) as period,
      COUNT(DISTINCT sa.customer_id) as active_customers,
      ROUND(SUM(t.amount), 2) as period_revenue,
      ROUND(AVG(t.amount), 2) as period_aov
    FROM `{TABLES['segment_assignments']}` sa
    JOIN `{TABLES['transactions']}` t ON sa.customer_id = t.customer_id
    GROUP BY sa.segment_id, sa.segment_name, year, quarter, period
    ORDER BY year, quarter, sa.segment_id
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '02_segment_health_over_time.csv'), index=False)
    print(f"  segment_health_over_time: {len(df)} rows")
    return df


def export_segment_migration(client):
    """View 2: Migration log data."""

    query = f"""
    SELECT *
    FROM `{TABLES['segment_migration_log']}`
    ORDER BY week
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '03_segment_migration.csv'), index=False)
    print(f"  segment_migration: {len(df)} rows")
    return df


def export_campaign_performance(client):
    """View 3: Campaign performance by segment — holdout test results."""

    query = f"""
    SELECT
      test_group,
      new_segment as segment_name,
      campaign_name,
      campaign_channel,
      COUNT(*) as sends,
      ROUND(AVG(opened) * 100, 1) as open_rate,
      ROUND(AVG(clicked) * 100, 1) as click_rate,
      ROUND(AVG(converted) * 100, 1) as conversion_rate,
      ROUND(SUM(revenue), 2) as total_revenue,
      ROUND(SUM(revenue) / COUNT(*), 2) as revenue_per_send
    FROM `{TABLES['campaign_holdout_results']}`
    GROUP BY test_group, new_segment, campaign_name, campaign_channel
    ORDER BY test_group, segment_name, campaign_name
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '04_campaign_performance.csv'), index=False)
    print(f"  campaign_performance: {len(df)} rows")
    return df


def export_campaign_summary(client):
    """View 3 continued: treatment vs control summary."""

    query = f"""
    SELECT
      test_group,
      COUNT(*) as total_sends,
      ROUND(AVG(opened) * 100, 1) as open_rate,
      ROUND(AVG(clicked) * 100, 1) as click_rate,
      ROUND(AVG(converted) * 100, 1) as conversion_rate,
      ROUND(SUM(revenue), 2) as total_revenue,
      ROUND(SUM(revenue) / COUNT(*), 2) as revenue_per_send
    FROM `{TABLES['campaign_holdout_results']}`
    GROUP BY test_group
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '05_campaign_summary.csv'), index=False)
    print(f"  campaign_summary: {len(df)} rows")
    return df


def export_segment_profiles(client):
    """View 4: Behavioral profile for each segment."""

    query = f"""
    SELECT
      sa.segment_id,
      sa.segment_name,
      cf.customer_id,
      cf.avg_basket_size,
      cf.median_basket_size,
      cf.weekly_purchase_frequency,
      cf.monthly_frequency,
      cf.total_transactions,
      cf.category_breadth,
      cf.online_ratio,
      cf.project_ratio,
      cf.promo_purchase_ratio,
      cf.avg_discount_depth,
      cf.full_price_ratio,
      cf.days_since_last_purchase,
      cf.frequency_trend,
      cf.q2_concentration,
      cf.weekend_ratio,
      cf.total_sessions,
      cf.search_rate,
      cf.deep_browse_ratio,
      cf.loyalty_tier_numeric,
      cf.points_balance,
      cf.region
    FROM `{TABLES['segment_assignments']}` sa
    JOIN `{TABLES['customer_features']}` cf ON sa.customer_id = cf.customer_id
    ORDER BY sa.segment_id, sa.customer_id
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '06_segment_profiles.csv'), index=False)
    print(f"  segment_profiles: {len(df)} rows")
    return df


def export_segment_profile_summary(client):
    """View 4 continued: aggregated profile per segment."""

    query = f"""
    SELECT
      sa.segment_id,
      sa.segment_name,
      COUNT(*) as customer_count,
      ROUND(AVG(cf.avg_basket_size), 2) as avg_basket,
      ROUND(AVG(cf.weekly_purchase_frequency), 4) as avg_weekly_freq,
      ROUND(AVG(cf.category_breadth), 1) as avg_categories,
      ROUND(AVG(cf.online_ratio), 3) as avg_online_ratio,
      ROUND(AVG(cf.project_ratio), 3) as avg_project_ratio,
      ROUND(AVG(cf.promo_purchase_ratio), 3) as avg_promo_ratio,
      ROUND(AVG(cf.avg_discount_depth), 3) as avg_discount_depth,
      ROUND(AVG(cf.full_price_ratio), 3) as avg_full_price_ratio,
      ROUND(AVG(cf.days_since_last_purchase), 0) as avg_recency_days,
      ROUND(AVG(cf.frequency_trend), 3) as avg_freq_trend,
      ROUND(AVG(cf.weekend_ratio), 3) as avg_weekend_ratio,
      ROUND(AVG(cf.total_sessions), 1) as avg_sessions,
      ROUND(AVG(cf.search_rate), 3) as avg_search_rate,
      ROUND(AVG(cf.deep_browse_ratio), 3) as avg_deep_browse
    FROM `{TABLES['segment_assignments']}` sa
    JOIN `{TABLES['customer_features']}` cf ON sa.customer_id = cf.customer_id
    GROUP BY sa.segment_id, sa.segment_name
    ORDER BY sa.segment_id
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '07_segment_profile_summary.csv'), index=False)
    print(f"  segment_profile_summary: {len(df)} rows")
    return df


def export_churn_indicators(client):
    """View 5: Churn signals by segment."""

    query = f"""
    SELECT
      sa.segment_id,
      sa.segment_name,
      cf.customer_id,
      cf.days_since_last_purchase,
      cf.frequency_trend,
      cf.monthly_frequency,
      cf.total_transactions,
      cf.loyalty_tier_numeric,
      CASE
        WHEN cf.frequency_trend < -0.3 AND cf.days_since_last_purchase > 300 THEN 'High Risk'
        WHEN cf.frequency_trend < -0.1 OR cf.days_since_last_purchase > 400 THEN 'Medium Risk'
        ELSE 'Low Risk'
      END as churn_risk_level
    FROM `{TABLES['segment_assignments']}` sa
    JOIN `{TABLES['customer_features']}` cf ON sa.customer_id = cf.customer_id
    ORDER BY cf.days_since_last_purchase DESC
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '08_churn_indicators.csv'), index=False)
    print(f"  churn_indicators: {len(df)} rows")
    return df


def export_churn_summary(client):
    """View 5 continued: churn risk distribution by segment."""

    query = f"""
    WITH risk_flags AS (
      SELECT
        sa.segment_id,
        sa.segment_name,
        cf.customer_id,
        CASE
          WHEN cf.frequency_trend < -0.3 AND cf.days_since_last_purchase > 300 THEN 'High Risk'
          WHEN cf.frequency_trend < -0.1 OR cf.days_since_last_purchase > 400 THEN 'Medium Risk'
          ELSE 'Low Risk'
        END as churn_risk_level
      FROM `{TABLES['segment_assignments']}` sa
      JOIN `{TABLES['customer_features']}` cf ON sa.customer_id = cf.customer_id
    )
    SELECT
      segment_id,
      segment_name,
      churn_risk_level,
      COUNT(*) as customer_count,
      ROUND(COUNT(*) / SUM(COUNT(*)) OVER(PARTITION BY segment_name) * 100, 1) as pct_of_segment
    FROM risk_flags
    GROUP BY segment_id, segment_name, churn_risk_level
    ORDER BY segment_id, churn_risk_level
    """
    df = client.query(query).to_dataframe()
    df.to_csv(os.path.join(EXPORT_DIR, '09_churn_summary.csv'), index=False)
    print(f"  churn_summary: {len(df)} rows")
    return df


def main():
    client = bigquery.Client(project=GCP_PROJECT_ID)
    os.makedirs(EXPORT_DIR, exist_ok=True)

    print("Exporting data for Tableau dashboard...")
    print("=" * 50)

    export_segment_health(client)
    export_segment_health_over_time(client)
    export_segment_migration(client)
    export_campaign_performance(client)
    export_campaign_summary(client)
    export_segment_profiles(client)
    export_segment_profile_summary(client)
    export_churn_indicators(client)
    export_churn_summary(client)

    print("=" * 50)
    print(f"\nAll exports saved to {EXPORT_DIR}/")
    print("Ready for Tableau Public import.")


if __name__ == "__main__":
    main()
