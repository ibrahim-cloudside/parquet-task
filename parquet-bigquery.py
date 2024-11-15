from google.cloud import bigquery

def load_parquet_to_bigquery(gcs_uri, dataset_id, table_id, project_id):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Set up the job configuration with character map V2
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        use_avro_logical_types=True  # Enables character map V2
    )

    try:
        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for job to complete
        table = client.get_table(table_ref)
        print(f"Loaded {table.num_rows} rows into {table_id} in {dataset_id}.")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")

# Parameters
gcs_uri = "gs://ib_eval/netflix_titles.parquet"
dataset_id = "parquet_to_bq"
table_id = "parquet_bq"
project_id = "local-bliss-437207-f0"

# Run the function
load_parquet_to_bigquery(gcs_uri, dataset_id, table_id, project_id)