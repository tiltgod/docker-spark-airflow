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
import file_utils as fu
# [END import_module]

gcp_connection_config = default_config.gcp_connection_setting
upload_config = default_config.to_gcs_setting
files_config = upload_config.dir_name

# add date time to all files to dir
# fu.add_date_to_files(upload_config.dir_name)

# [START instantiate_dag]
with DAG(

    dag_id = "tempest_homework_dag",
    # [START default_args]
    default_args={"owner": "airflow",
    },
    schedule_interval=None,
    start_date=datetime(2023, 8, 29),
    # [END default_args]

) as dag:
    # [END instantiate_dag]

    # PythonOperator - create schema file yaml that has the list of file names of data_sorce's dir

    activateGCP = PythonOperator(
        task_id='add_gcp_connection_python',
        python_callable=create_gcp_connection,
        op_kwargs={'gcp_connection_config': gcp_connection_config},
    )

    add_date_to_files = PythonOperator(
        task_id='add_date_to_files',
        python_callable=fu.add_date_to_files,
        op_kwargs={'source_path':files_config}
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=upload_config.dir_name + "/*",
        dst="",
        bucket="homeworkbuckett",
    )

    activateGCP >> add_date_to_files >> [upload_file]
# [END tutorial]
