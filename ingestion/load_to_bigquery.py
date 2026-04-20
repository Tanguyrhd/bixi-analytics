from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DATASET_ID = "raw"

FILES = ["2023", "2024", "2025"]

SCHEMA = [
    bigquery.SchemaField("STARTSTATIONNAME", "STRING"),
    bigquery.SchemaField("STARTSTATIONARRONDISSEMENT", "STRING"),
    bigquery.SchemaField("STARTSTATIONLATITUDE", "FLOAT"),
    bigquery.SchemaField("STARTSTATIONLONGITUDE", "FLOAT"),
    bigquery.SchemaField("ENDSTATIONNAME", "STRING"),
    bigquery.SchemaField("ENDSTATIONARRONDISSEMENT", "STRING"),
    bigquery.SchemaField("ENDSTATIONLATITUDE", "FLOAT"),
    bigquery.SchemaField("ENDSTATIONLONGITUDE", "FLOAT"),
    bigquery.SchemaField("STARTTIMEMS", "INT64"),
    bigquery.SchemaField("ENDTIMEMS", "INT64"),
]

def load_file(client, year):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.trips_{year}"
    uri = f"gs://{BUCKET_NAME}/raw/{year}.csv"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Loading {uri} → {table_id}")
    job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    job.result()
    print(f"Done: {client.get_table(table_id).num_rows} rows loaded")

def main():
    client = bigquery.Client(project=PROJECT_ID)
    for year in FILES:
        load_file(client, year)
    print("\nAll tables loaded successfully!")

if __name__ == "__main__":
    main()
