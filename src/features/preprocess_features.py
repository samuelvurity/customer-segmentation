"""
Preprocess customer features for clustering.

Reads from BigQuery customer_features table, applies:
1. Feature selection (clustering features vs context)
2. Log transform on right-skewed features
3. Standard scaling
4. Writes processed features back to BigQuery

Production note: At HD scale, rolling 8-12 week windows were used
instead of lifetime features. Simulation uses lifetime due to data density.
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from google.cloud import bigquery
import joblib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.project_config import GCP_PROJECT_ID, BQ_DATASET, TABLES

CLUSTERING_FEATURES = [
    "weekly_purchase_frequency",
    "category_breadth",
    "avg_basket_size",
    "online_ratio",
    "project_ratio",
    "promo_purchase_ratio",
    "avg_discount_depth",
    "full_price_ratio",
    "days_since_last_purchase",
    "monthly_frequency",
    "frequency_trend",
    "q2_concentration",
    "weekend_ratio",
    "total_sessions",
    "search_rate",
    "product_page_ratio",
    "deep_browse_ratio",
]

LOG_TRANSFORM_FEATURES = [
    "avg_basket_size",
    "days_since_last_purchase",
    "total_sessions",
]

CONTEXT_COLUMNS = [
    "customer_id",
    "region",
]


def read_features_from_bq(client):
    query = f"SELECT * FROM `{TABLES['customer_features']}`"
    df = client.query(query).to_dataframe()
    print(f"Read {len(df)} rows from BigQuery.")
    return df


def apply_log_transform(df, features):
    df_out = df.copy()
    for col in features:
        if col in df_out.columns:
            df_out[col] = np.log1p(df_out[col])
    print(f"Log-transformed {len(features)} features.")
    return df_out


def apply_standard_scaling(df, features):
    scaler = StandardScaler()
    df_out = df.copy()
    df_out[features] = scaler.fit_transform(df_out[features])
    print(f"Standard-scaled {len(features)} features.")
    return df_out, scaler


def write_processed_to_bq(client, df):
    table_ref = TABLES["customer_features_processed"]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"Wrote {table.num_rows} rows to {table_ref}.")


def main():
    client = bigquery.Client(project=GCP_PROJECT_ID)

    print("Step 1: Reading features from BigQuery...")
    df_raw = read_features_from_bq(client)

    print("\nStep 2: Selecting clustering features...")
    df_clustering = df_raw[CONTEXT_COLUMNS + CLUSTERING_FEATURES].copy()

    null_counts = df_clustering[CLUSTERING_FEATURES].isnull().sum()
    if null_counts.any():
        print(f"  Nulls found:\n{null_counts[null_counts > 0]}")
        df_clustering[CLUSTERING_FEATURES] = df_clustering[CLUSTERING_FEATURES].fillna(0)
        print("  Filled nulls with 0.")
    else:
        print("  No nulls found.")

    print("\nStep 3: Feature distributions before transforms...")
    skewed = df_clustering[LOG_TRANSFORM_FEATURES].describe().loc[["mean", "50%", "max"]]
    print(skewed.to_string())

    print("\nStep 4: Applying log transform...")
    df_transformed = apply_log_transform(df_clustering, LOG_TRANSFORM_FEATURES)

    print("\nStep 5: Applying standard scaling...")
    df_scaled, scaler = apply_standard_scaling(df_transformed, CLUSTERING_FEATURES)

    print("\nStep 6: Saving scaler artifact...")
    artifacts_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'artifacts')
    os.makedirs(artifacts_dir, exist_ok=True)
    scaler_path = os.path.join(artifacts_dir, "feature_scaler.joblib")
    joblib.dump(scaler, scaler_path)
    print(f"  Scaler saved to {scaler_path}")

    print("\nStep 7: Feature distributions after transforms...")
    print(df_scaled[CLUSTERING_FEATURES].describe().loc[["mean", "std", "min", "max"]].to_string())

    print("\nStep 8: Writing processed features to BigQuery...")
    write_processed_to_bq(client, df_scaled)

    print("\nPreprocessing complete.")


if __name__ == "__main__":
    main()
