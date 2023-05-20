import csv
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator , BranchPythonOperator
from airflow.utils import timezone
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from airflow import models
from airflow.operators.dummy import DummyOperator

import logging


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
PROJECT_ID = "dataengineerbootcamp"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "users"
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
    #  "user_id": "bbe51ac6-6687-4cbe-9178-27d96f90836b",
    #     "first_name": "Keslie",
    #     "last_name": "Hearmon",
    #     "email": "khearmon0@netvibes.com",
    #     "phone_number": "831-155-1615",
    #     "created_at": "2020-10-23T20:21:57Z",
    #     "updated_at": "2021-01-30T22:49:31Z",
    #     "address": "7a4821e6-4e7a-4894-bb35-70ffcf0c3aa8"
   
    if data:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "user_id",
                "first_name",
                "last_name",
                "email",
                "phone_number",
                "created_at",
                "updated_at",
                "address",
            ]
            writer.writerow(header)
            for each in data:
                # print(each["event_id"], each["event_type"])
                data = [
                    each["user_id"],
                    each["first_name"],
                    each["last_name"],
                    each["email"],
                    each["phone_number"],
                    each["created_at"],
                    each["updated_at"],
                    each["address"]
                ]
                writer.writerow(data)# Return a value indicating data is available
        return 'load_data_to_gcs'
    else:
        return 'no_data_task'  # Return a value indicating no data
    


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
        clustering_fields=["first_name","last_name"],
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
    "start_date": timezone.datetime(2020, 1, 5),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_users_data_pipeline",  # Replace xxx with the data name
    default_args=default_args,
    schedule="@daily",  # Set your schedule here
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):
    # Extract data from Postgres, API, or SFTP
    extract_data = BranchPythonOperator(
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
    
    no_data_task = DummyOperator(
        task_id='no_data_task',
    
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery
    extract_data >> no_data_task

#airflow dags backfill -s 2020-01-05 -e 2020-12-26 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-01-05 -e 2020-01-31 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-02-01 -e 2020-02-28 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-03-01 -e 2020-03-31 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-04-01 -e 2020-04-30 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-05-01 -e 2020-05-31 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-06-01 -e 2020-06-30 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-07-01 -e 2020-07-31 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-08-01 -e 2020-08-31 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-09-01 -e 2020-09-30 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-10-01 -e 2020-10-31 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-11-01 -e 2020-11-30 greenery_users_data_pipeline --reset-dagruns
#airflow dags backfill -s 2020-12-01 -e 2020-12-26 greenery_users_data_pipeline --reset-dagruns