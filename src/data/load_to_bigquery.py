from google.cloud import bigquery
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.project_config import GCP_PROJECT_ID, BQ_DATASET, BQ_LOCATION
from data.mock.generate_mock_data import generate_all

DATE_COLUMNS = {
    "customer_master": ["signup_date"],
    "transactions": ["transaction_date"],
    "web_events": ["event_date"],
    "loyalty": ["enrollment_date", "last_activity_date"],
    "old_segments": ["assignment_date"],
    "campaigns": ["send_date"],
}

SCHEMAS = {
    "customer_master": [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age_bracket", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("signup_date", "DATE"),
    ],
    "transactions": [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("transaction_date", "DATE"),
        bigquery.SchemaField("amount", "FLOAT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("promo_flag", "BOOLEAN"),
        bigquery.SchemaField("discount_pct", "FLOAT64"),
    ],
    "web_events": [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("page_type", "STRING"),
        bigquery.SchemaField("device", "STRING"),
        bigquery.SchemaField("search_flag", "INT64"),
    ],
    "loyalty": [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("loyalty_tier", "STRING"),
        bigquery.SchemaField("points_balance", "INT64"),
        bigquery.SchemaField("enrollment_date", "DATE"),
        bigquery.SchemaField("last_activity_date", "DATE"),
    ],
    "old_segments": [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("old_segment_id", "INT64"),
        bigquery.SchemaField("segment_name", "STRING"),
        bigquery.SchemaField("assignment_date", "DATE"),
    ],
    "campaigns": [
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("send_date", "DATE"),
        bigquery.SchemaField("segment_targeted", "INT64"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("opened", "INT64"),
        bigquery.SchemaField("clicked", "INT64"),
        bigquery.SchemaField("converted", "INT64"),
        bigquery.SchemaField("revenue", "FLOAT64"),
    ],
}


def create_dataset(client):
    dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = BQ_LOCATION
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_ref} ready.")
    return dataset


def convert_date_columns(df, table_name):
    if table_name in DATE_COLUMNS:
        for col in DATE_COLUMNS[table_name]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


def load_table(client, df, table_name):
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    schema = SCHEMAS[table_name]

    df = convert_date_columns(df, table_name)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    job.result()

    table = client.get_table(table_ref)
    print(f"  {table_name}: {table.num_rows:,} rows loaded.")


def main():
    client = bigquery.Client(project=GCP_PROJECT_ID)

    print("Creating dataset...")
    create_dataset(client)

    print("\nGenerating mock data...")
    data = generate_all()

    print("\nLoading tables to BigQuery...")
    for table_name, df in data.items():
        load_table(client, df, table_name)

    print("\nAll tables loaded. Verifying row counts...")
    for table_name in data.keys():
        table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
        table = client.get_table(table_ref)
        print(f"  {table_name}: {table.num_rows:,} rows")

    print("\nDone.")


if __name__ == "__main__":
    main()
