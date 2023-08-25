from __future__ import annotations

# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent
import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from tempest_homework_dag_config import default_config

from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from create_gcp_connection import create_gcp_connection
from add_date_to_destination_path import add_date_to_destination_path
# [END import_module]


gcp_connection_config = default_config.gcp_connection_setting
upload_config = default_config.to_gcs_setting
src_file_path = upload_config.dir_name + upload_config.src_name
destination_path = add_date_to_destination_path(src_file_path)

# [START instantiate_dag]
with DAG(

    dag_id = "tempest_homework_dag",
    # [START default_args]
    default_args={"owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 25),
    "end_date": datetime(2023, 8, 26),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "schedule_interval": "@once"
    },
    
    # [END default_args]

) as dag:
    # [END instantiate_dag]

    activateGCP = PythonOperator(
        task_id='add_gcp_connection_python',
        python_callable=create_gcp_connection,
        op_kwargs={'gcp_connection_config': gcp_connection_config},
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=src_file_path,
        dst=destination_path,
        bucket="homeworkbuckett",
    )

    activateGCP > upload_file
# [END tutorial]
