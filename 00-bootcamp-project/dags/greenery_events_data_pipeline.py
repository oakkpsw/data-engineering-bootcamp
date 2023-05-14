import csv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.models import Variable

import requests


# Import modules regarding GCP service account, BigQuery, and GCS 
# Your code here


def _extract_data(ds):
    url = f"http://34.87.139.82:8000/events/?created_at={ds}"
    response = requests.get(url)
    data = response.json()

    with open(f"/opt/airflow/dags/events-{ds}.csv", "w") as f:
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


def _load_data_to_gcs():
    # Your code below
    # api_config_host = "{{ var.value.api_config_host }}"
    # print("xx")
    # print(api_config_host)
    keyfile_gcs = ""
    # keyfile_gcs = "dataengineerbootcamp-f3228d1da17f-gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)
    pass


def _load_data_from_gcs_to_bigquery():
    # Your code below
    pass


default_args = {
    "owner": "airflow",
        "start_date": timezone.datetime(2023, 5, 1),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_events_data_pipeline",  # Replace xxx with the data name
    default_args=default_args,
    schedule=None,  # Set your schedule here
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
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery