# Gold Layer: Loading to BigQuery / Production DB
from google.cloud import bigquery

def load_to_bigquery(source_path, table_id):
    """
    Loads Parquet files from Silver/Gold layer into BigQuery.
    """
    client = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )
    
    # load_job = client.load_table_from_uri(source_path, table_id, job_config=job_config)
    print(f"Loading {source_path} to {table_id}...")

if __name__ == "__main__":
    # load_to_bigquery("warehouse/data/silver/telemetry.parquet", "your_project.f1.telemetry")
    pass
