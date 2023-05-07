# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

import json
import os
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "dataengineerbootcamp"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)


# load direct from csv
def load_data(files):
    for file in files:
        # partitioned table
        clustering_fields_setting = None
        if file in ["events","orders","users"]:
            if file == "users":
                clustering_fields_setting=["first_name", "last_name"]
                dt = "2020-10-23"
            elif file == "events":
                dt = "2021-02-10"
            elif file == "orders":
                dt = "2021-02-10"
            load_data_with_partitioned(file,dt,clustering_fields_setting)
        else:
            load_data_without_partitioned(file)


def load_data_with_partitioned(file,dt,clustering_fields_setting):
    file_path = f"data/{file}.csv" 
    with open(file_path, "rb") as f:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="created_at",
                    ),
            clustering_fields=clustering_fields_setting,
        )
        partition = dt.replace("-", "")
        table_id = f"{project_id}.deb_bootcamp.{file}${partition}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def load_data_without_partitioned(file):
    file_path = f"data/{file}.csv" 
    with open(file_path, "rb") as f:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        table_id = f"{project_id}.deb_bootcamp.{file}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

if __name__ == "__main__":
    file_name = ["addresses","events","order_items","orders","products","promos","users"]
    # file_name = ["events"]
    load_data(file_name)