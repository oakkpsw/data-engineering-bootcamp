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
DATA = "orders"
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
    print(data)
#  "order_id": "8329a65b-7ddf-4250-aeee-bd625f8a401a",
#         "created_at": "2021-02-11T23:30:34Z",
#         "order_cost": 551.9,
#         "shipping_cost": 5.16,
#         "order_total": 557.06,
#         "tracking_id": "1b2b3cff-dec1-47f6-a507-a64de9ddc663",
#         "shipping_service": "ups",
#         "estimated_delivery_at": "2021-02-12T23:30:34Z",
#         "delivered_at": "2021-02-17T23:30:34Z",
#         "status": "delivered",
#         "user": "b3367c91-53bd-4aac-ab6d-0a596fe382c2",
#         "promo": null,
#         "address": "d2fbe240-64ac-4feb-a360-8a9197f8b8ae"
    with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
        writer = csv.writer(f)
        header = [
            "order_id",
            "created_at",
            "order_cost",
            "shipping_cost",
            "order_total",
            "tracking_id",
            "shipping_service",
            "estimated_delivery_at",
            "delivered_at",
            "status",
            "user",
            "promo",
            "address",
        ]
        writer.writerow(header)
        for each in data:
            # print(each["event_id"], each["event_type"])
            data = [
                each["order_id"],
                each["created_at"],
                each["order_cost"],
                each["shipping_cost"],
                each["order_total"],
                each["tracking_id"],
                each["shipping_service"],
                each["estimated_delivery_at"],
                each["delivered_at"],
                each["status"],
                each["user"],
                each["promo"],
                each["address"],
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
    

#2020-01-05
default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2022, 2, 10),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_orders_data_pipeline",  # Replace xxx with the data name
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


#airflow dags backfill -s 2021-02-10 -e 2021-02-11 greenery_orders_data_pipeline --reset-dagruns
