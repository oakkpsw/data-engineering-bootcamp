import csv
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from airflow import models
import logging


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
PROJECT_ID = "dataengineerbootcamp"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "events"
BUCKET_NAME = "deb-bootcamp-100028"

# Import modules regarding GCP service account, BigQuery, and GCS 
# Your code here

# variable from airflow 
host = models.Variable.get('api_config_host') 
port = models.Variable.get('api_config_port') 
api_url = f"http://{host}:{port}"

keyfile_gcs_name = models.Variable.get('gcs_key')
keyfile_bcq_name = models.Variable.get('bcq_key')
def _extract_data(ds):
    url = f"{api_url}/{DATA}/?created_at={ds}"
    # print(url)
    # logging.info(f"URL: {url}")  # Log the URL
    response = requests.get(url)
    data = response.json()
    with open(f"{DAGS_FOLDER}/events-{ds}.csv", "w") as f:
        writer = csv.writer(f)
        header = [
            "event_id",
            "session_id",
            "page_url",
            "created_at",
            "event_type",
            "user",
            "order",
            "product",
        ]
        writer.writerow(header)
        for each in data:
            # print(each["event_id"], each["event_type"])
            data = [
                each["event_id"],
                each["session_id"],
                each["page_url"],
                each["created_at"],
                each["event_type"],
                each["user"],
                each["order"],
                each["product"]
            ]
            writer.writerow(data)


def _load_data_to_gcs(ds):
    # Your code below
    #settings
    #bcq
    #gcs
    # keyfile_gcs_name = "{{ var.value.gcs_key }}"
    service_account_info_gcs = json.load(open(f"{DAGS_FOLDER}/{keyfile_gcs_name}"))
    credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(BUCKET_NAME)
    file_path = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    


def _load_data_from_gcs_to_bigquery(ds):

    service_account_info_bcq = json.load(open(f"{DAGS_FOLDER}/{keyfile_bcq_name}"))
    credentials_bcq = service_account.Credentials.from_service_account_info(service_account_info_bcq)
    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials_bcq,
        location=LOCATION,
    )
    partition = ds.replace("-", "")
    table_id = f"{PROJECT_ID}.deb_bootcamp.{DATA}${partition}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at",
                ),
    )
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    job = bigquery_client.load_table_from_uri(
        f"gs://{BUCKET_NAME}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()
    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_events_data_pipeline",  # Replace xxx with the data name
    default_args=default_args,
    schedule="@daily",  # Set your schedule here
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):
    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery