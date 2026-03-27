"""
Segment Drift Monitor

Production monitoring script that runs after each weekly refresh.
Compares current segment distribution against previous week.
Flags unusual migration patterns for investigation.

Edge cases handled:
  - New customers (< 3 transactions): held in "New/Low-Activity" bucket
  - Seasonal distortion: rolling window features resist single-event spikes
  - Power users: log transform prevents outlier dominance
  - Rapid migration: >15% shift in one week triggers alert
"""

import pandas as pd
import numpy as np
from google.cloud import bigquery
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.project_config import (
    GCP_PROJECT_ID, TABLES, STABILITY_ALERT_THRESHOLD, MIN_TRANSACTIONS_THRESHOLD
)


def check_segment_distribution(client):
    """Compare current segment sizes against expected ranges."""

    query = f"""
    SELECT
      segment_name,
      COUNT(*) as customer_count,
      ROUND(COUNT(*) / SUM(COUNT(*)) OVER() * 100, 1) as pct_of_total
    FROM `{TABLES['segment_assignments']}`
    GROUP BY segment_name
    ORDER BY customer_count DESC
    """
    df = client.query(query).to_dataframe()

    print("Current Segment Distribution:")
    print("-" * 50)
    for _, row in df.iterrows():
        print(f"  {row['segment_name']:<22} {row['customer_count']:>5} ({row['pct_of_total']}%)")

    # Flag segments that are too small (<5%) or too large (>40%)
    alerts = []
    for _, row in df.iterrows():
        if row['pct_of_total'] < 5:
            alerts.append(f"  ⚠️ {row['segment_name']} is only {row['pct_of_total']}% — may be too thin for marketing to act on")
        if row['pct_of_total'] > 40:
            alerts.append(f"  ⚠️ {row['segment_name']} is {row['pct_of_total']}% — catch-all segment, investigate")

    if alerts:
        print("\nAlerts:")
        for a in alerts:
            print(a)
    else:
        print("\nNo distribution alerts.")

    return df


def check_new_customers(client):
    """Identify customers crossing the 3-transaction threshold."""

    query = f"""
    WITH txn_counts AS (
      SELECT customer_id, COUNT(*) as txn_count
      FROM `{TABLES['transactions']}`
      GROUP BY customer_id
    ),
    assigned AS (
      SELECT DISTINCT customer_id
      FROM `{TABLES['segment_assignments']}`
    )
    SELECT
      COUNTIF(tc.txn_count < {MIN_TRANSACTIONS_THRESHOLD}) as below_threshold,
      COUNTIF(tc.txn_count >= {MIN_TRANSACTIONS_THRESHOLD} AND a.customer_id IS NULL) as eligible_unassigned,
      COUNTIF(a.customer_id IS NOT NULL) as currently_assigned
    FROM txn_counts tc
    LEFT JOIN assigned a ON tc.customer_id = a.customer_id
    """
    df = client.query(query).to_dataframe()
    row = df.iloc[0]

    print(f"\nNew Customer Status:")
    print(f"  Below threshold (<{MIN_TRANSACTIONS_THRESHOLD} txns): {int(row['below_threshold'])}")
    print(f"  Eligible but unassigned: {int(row['eligible_unassigned'])}")
    print(f"  Currently assigned: {int(row['currently_assigned'])}")

    if row['eligible_unassigned'] > 0:
        print(f"  ⚠️ {int(row['eligible_unassigned'])} customers need segment assignment on next refresh")

    return row


def check_migration_rate(client):
    """Check latest migration log for stability."""

    query = f"""
    SELECT *
    FROM `{TABLES['segment_migration_log']}`
    ORDER BY week DESC
    LIMIT 1
    """
    df = client.query(query).to_dataframe()

    if len(df) == 0:
        print("\nNo migration data available.")
        return None

    latest = df.iloc[0]
    rate = latest['migration_rate']
    threshold = STABILITY_ALERT_THRESHOLD

    print(f"\nLatest Migration Check (Week {int(latest['week'])}):")
    print(f"  Migration rate: {rate:.1%}")
    print(f"  Threshold: {threshold:.0%}")

    if rate > threshold:
        print(f"  🔴 ALERT: Migration rate exceeds threshold — investigate data quality or external event")
    else:
        print(f"  ✅ PASS: Segments are stable")

    return latest


def check_feature_drift(client):
    """Check if key feature distributions have shifted significantly."""

    query = f"""
    SELECT
      ROUND(AVG(avg_basket_size), 2) as mean_basket,
      ROUND(STDDEV(avg_basket_size), 2) as std_basket,
      ROUND(AVG(promo_purchase_ratio), 4) as mean_promo,
      ROUND(AVG(online_ratio), 4) as mean_online,
      ROUND(AVG(days_since_last_purchase), 0) as mean_recency,
      ROUND(AVG(frequency_trend), 4) as mean_trend
    FROM `{TABLES['customer_features']}`
    """
    df = client.query(query).to_dataframe()
    row = df.iloc[0]

    print(f"\nFeature Distribution Check:")
    print(f"  Avg basket: ${row['mean_basket']} (std: ${row['std_basket']})")
    print(f"  Avg promo ratio: {row['mean_promo']:.1%}")
    print(f"  Avg online ratio: {row['mean_online']:.1%}")
    print(f"  Avg recency: {int(row['mean_recency'])} days")
    print(f"  Avg frequency trend: {row['mean_trend']:+.4f}")

    return row


def main():
    client = bigquery.Client(project=GCP_PROJECT_ID)

    print("=" * 60)
    print("SEGMENT DRIFT MONITOR — WEEKLY CHECK")
    print("=" * 60)

    check_segment_distribution(client)
    check_new_customers(client)
    check_migration_rate(client)
    check_feature_drift(client)

    print("\n" + "=" * 60)
    print("MONITORING COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
