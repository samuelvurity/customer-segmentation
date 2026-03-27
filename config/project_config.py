GCP_PROJECT_ID = "hd-segmentation-sim"
BQ_DATASET = "hd_segmentation"
BQ_LOCATION = "US"

TABLES = {
    "transactions": f"{GCP_PROJECT_ID}.{BQ_DATASET}.transactions",
    "web_events": f"{GCP_PROJECT_ID}.{BQ_DATASET}.web_events",
    "loyalty": f"{GCP_PROJECT_ID}.{BQ_DATASET}.loyalty",
    "customer_master": f"{GCP_PROJECT_ID}.{BQ_DATASET}.customer_master",
    "old_segments": f"{GCP_PROJECT_ID}.{BQ_DATASET}.old_segments",
    "campaigns": f"{GCP_PROJECT_ID}.{BQ_DATASET}.campaigns",
    "customer_features": f"{GCP_PROJECT_ID}.{BQ_DATASET}.customer_features",
    "customer_features_processed": f"{GCP_PROJECT_ID}.{BQ_DATASET}.customer_features_processed",
    "segment_assignments": f"{GCP_PROJECT_ID}.{BQ_DATASET}.segment_assignments",
    "campaign_holdout_results": f"{GCP_PROJECT_ID}.{BQ_DATASET}.campaign_holdout_results",
    "segment_migration_log": f"{GCP_PROJECT_ID}.{BQ_DATASET}.segment_migration_log",
}

N_CUSTOMERS = 1000
N_TRANSACTIONS = 15000
N_WEB_EVENTS = 50000
N_CAMPAIGNS = 5000

MIN_TRANSACTIONS_THRESHOLD = 3
K_CLUSTERS = 5
ROLLING_WINDOW_WEEKS = 12
STABILITY_ALERT_THRESHOLD = 0.15
