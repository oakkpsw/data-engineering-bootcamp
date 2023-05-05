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

#users  use pandas 
# job_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     schema=[
#         bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
#         bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
#         bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
#         bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
#     ],
#     time_partitioning=bigquery.TimePartitioning(
#         type_=bigquery.TimePartitioningType.DAY,
#         field="created_at",
#     ),
#     clustering_fields=["first_name", "last_name"],
# )
# folder_name = "data"
# file_path = f"{folder_name}/users.csv"
# df = pd.read_csv(file_path, parse_dates=["created_at", "updated_at"])
# df.info()

# table_id = f"{project_id}.deb_bootcamp.users"
# job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
# job.result()

# table = client.get_table(table_id)
# print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

# load direct from csv

# no partition






def upload(files,folder_name):
    for file in files:
        file_path = f"{folder_name}/{file}.csv"
        # partitioned table
        
        if file in ["events","orders","users"]:
            time_partition_setting=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at",
                )
            if file == "users":
                clustering_fields_setting=["first_name", "last_name"]
            elif file == "events":
                clustering_fields_setting=["user"]
            elif file == "orders":
                clustering_fields_setting=["user"]
        else:
            time_partition_setting= None
            clustering_fields_setting = None
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            time_partitioning=time_partition_setting,
            clustering_fields=clustering_fields_setting,
        )
        with open(file_path, "rb") as f:
            table_id = f"{project_id}.deb_bootcamp.{file}"
            job = client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()

        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

if __name__ == "__main__":
    file_name = ["addresses","events","order_items","orders","products","promos","users"]
    folder_name = "data"
    upload(file_name,folder_name)