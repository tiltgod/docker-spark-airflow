from __future__ import annotations
from datetime import datetime
from airflow import DAG
from tempest_homework_dag_config import default_config
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from application.local_to_gcs import local_to_gcs
import json

# global variables
gcp_connection_config = default_config.gcp_connection_setting
upload_config = default_config.to_gcs_setting
local_dir = upload_config.dir_name
gcp_bucket_name = upload_config.bucket_name
gcp_bucket_credential_path = gcp_connection_config.bucket_keypath
gcp_bq_credential_path = gcp_connection_config.bq_keypath

# read credential to pass into spark application
with open(gcp_bucket_credential_path) as json_file:
    data = json.load(json_file)
    gcp_bucket_credential = data
    str_gcp_bucket_credential = str(data)

with open(gcp_bq_credential_path) as json_file_1:
    data = json.load(json_file_1)
    str_gcp_bq_credential = str(data)

spark_args = [gcp_bucket_name, str_gcp_bucket_credential, str_gcp_bq_credential]


with DAG(
    dag_id = "tempest_homework_dag",
    default_args={"owner": "airflow",
    },
    schedule_interval=None,
    start_date=datetime(2023, 8, 29),
    
) as dag:

    # upload_file = PythonOperator(
    #     task_id="upload_file",
    #     python_callable=local_to_gcs,
    #     op_kwargs={'credential_info': gcp_bucket_credential, 'bucket_name':gcp_bucket_name, 'local_dir': local_dir},
    # )


    spark_transform = SparkSubmitOperator(
		application = "/opt/airflow/dags/spark_transform_script.py",
		conn_id= 'spark', 
		task_id='spark_submit_task',
        application_args=spark_args
	)

    # task dependencies
    # upload_file >> spark_transform
