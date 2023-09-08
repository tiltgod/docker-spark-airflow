from __future__ import annotations
from datetime import datetime
from airflow import DAG
from tempest_homework_dag_config import default_config
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from create_gcp_connection import create_gcp_connection
import file_utils as fu
import json

gcp_connection_config = default_config.gcp_connection_setting
upload_config = default_config.to_gcs_setting
files_config = upload_config.dir_name
gcp_bucket = upload_config.bucket_name
gcp_credential_path = gcp_connection_config.keypath

with open(gcp_credential_path) as json_file:
    data = json.load(json_file)
    gcp_credential = str(data)

spark_args = [gcp_bucket, gcp_credential]


with DAG(
    dag_id = "tempest_homework_dag",
    default_args={"owner": "airflow",
    },
    schedule_interval=None,
    start_date=datetime(2023, 8, 29),
    
) as dag:

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

    # sparkoperator

    spark_transform = SparkSubmitOperator(
		application = "/opt/airflow/dags/spark_transform_script.py",
		conn_id= 'spark', 
		task_id='spark_submit_task',
        application_args=spark_args
		)

    spark_transform
