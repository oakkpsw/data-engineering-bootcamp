import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
location = "asia-southeast1"
project_id = "dataengineerbootcamp"

#settings
#bcq
keyfile_bcq = os.environ.get("KEYFILE_PATH_BCQ")
service_account_info_bcq = json.load(open(keyfile_bcq))
credentials_bcq = service_account.Credentials.from_service_account_info(service_account_info_bcq)
bigquery_client = bigquery.Client(
    project=project_id,
    credentials=credentials_bcq,
    location=location,
)

# Load data from Local to GCS
bucket_name = "deb-bootcamp-100028"
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

def load_data(datas):
    for data in datas:
        file_path = f"{DATA_FOLDER}/{data}.csv"
        destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        # partitioned table
        clustering_fields_setting = None
        if data in ["events","orders","users"]:
            if data == "users":
                clustering_fields_setting=["first_name", "last_name"]
                dt = "2020-10-23"
            elif data == "events":
                dt = "2021-02-10"
            elif data == "orders":
                dt = "2021-02-10"
            load_data_with_partitioned(data,destination_blob_name,dt,clustering_fields_setting)
        else:
            load_data_without_partitioned(data,destination_blob_name)


def load_data_without_partitioned(data,destination_blob_name):
    table_id = f"{project_id}.deb_bootcamp.{data}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()
    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def load_data_with_partitioned(data,destination_blob_name,dt,clustering_fields_setting):
    partition = dt.replace("-", "")
    table_id = f"{project_id}.deb_bootcamp.{data}${partition}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at",
                ),
        clustering_fields=clustering_fields_setting,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()
    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

if __name__ == "__main__":
    file_name = ["addresses","events","order_items","orders","products","promos","users"]
    load_data(file_name)