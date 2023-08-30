from __future__ import annotations
from datetime import datetime
from airflow import DAG
from tempest_homework_dag_config import default_config
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from create_gcp_connection import create_gcp_connection
import file_utils as fu

gcp_connection_config = default_config.gcp_connection_setting
upload_config = default_config.to_gcs_setting
files_config = upload_config.dir_name

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

    test_spark = SparkSubmitOperator(
		application = "/opt/airflow/dags/spark_etl_script_docker.py",
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=spark_dag
		)

    activateGCP >> add_date_to_files >> [upload_file]
