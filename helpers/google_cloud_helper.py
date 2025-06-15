from google.cloud import storage, bigquery


def load_file_to_gcs(file_path: str, bucket_name: str, destination_blob_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)



def load_parquet_to_bq(dataset_id, table_id, gcs_uri):
    client = bigquery.Client()
    full_table_id = f"{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        full_table_id,
        job_config=job_config,
    )
    load_job.result()