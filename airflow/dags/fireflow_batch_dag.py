import os
import logging
import requests
import json
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


dataset_file = "acs-usa.csv"
dataset_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# path_to_local_home = "/opt/airflow/"
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'homework_modul2')

# GITHUB DATA URL
customer = 'https://raw.githubusercontent.com/billiechristian/data-fellowship-fireflow/main/data/customer.csv'
education = 'https://raw.githubusercontent.com/billiechristian/data-fellowship-fireflow/main/data/education.csv'
job = 'https://raw.githubusercontent.com/billiechristian/data-fellowship-fireflow/main/data/job.csv'
salesperson = 'https://raw.githubusercontent.com/billiechristian/data-fellowship-fireflow/main/data/salesperson.csv'
sales_training = 'https://raw.githubusercontent.com/billiechristian/data-fellowship-fireflow/main/data/salesperson_training.csv'
training_course = 'https://raw.githubusercontent.com/billiechristian/data-fellowship-fireflow/main/data/training_course.csv'
fact = f'https://raw.githubusercontent.com/billiechristian/data-fellowship-fireflow/main/data/data_split/fact_table_{date}.csv'

url_list = [customer, education, job, salesperson, sales_training, training_course, fact]
local_file_name = ['customer.csv', 'education.csv', 'job.csv', 'salesperson.csv', 'sales_training.csv', 'training_course.csv', f'fact_table_{date}.csv']

def download_github_data (URL_LIST, LOCAL_FILE_NAME):
    for url, filename in zip(URL_LIST, LOCAL_FILE_NAME):
        os.system(f"wget {url} -O {path_to_local_home}/{filename}")

def load_data_to_gcs(bucket, object_name, local_file_name):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    for filename, object in zip(local_file_name, object_name) :
        blob = bucket.blob(object)
        blob.upload_from_filename(filename)



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="fireflow_dag",
    schedule_interval="0 10 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['data-fellowship-8'],
) as dag:

    download_github_data_task = PythonOperator(
        task_id="download_github_data_task",
        python_callable=download_github_data,
        op_kwargs={
            'URL_LIST': url_list,
            'LOCAL_FILE_NAME': local_file_name},
    )

    load_to_gcs = PythonOperator(
            task_id="load_to_gcs_task",
            python_callable=load_data_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"data/{local_file_name}",
                "local_file_name": f"{path_to_local_home}/{local_file_name}",
            },
    ),

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    download_github_data_task >> format_to_csv_task >> \
        format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task


# Overall DAG flow
# download_dataset_from_github (daily) 
# >> save_to_GCS 
# >> load_to_bigquery
# >> run_dbt

