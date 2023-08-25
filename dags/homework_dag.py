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
# [END import_module]

src_file_path = "/opt/airflow/data_source/inpatient_charges_2012.csv"
gcp_connection_config = default_config.gcp_connection_setting
print(str(gcp_connection_config))

def add_date_to_destination(source_path):
    my_datetime = datetime.datetime.today()
    my_date = my_datetime.date()
    my_date.strftime("%Y%d%m")
    # get only file name from path
    os.path.basename(source_path)
    bucket_desination =  my_date.strftime("%Y%d%m") +"/"+ os.path.basename(source_path)
    return bucket_desination


# [START instantiate_dag]
with DAG(

    dag_id = "homework_dag",
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

    # upload_file = LocalFilesystemToGCSOperator(
    #     task_id="upload_file",
    #     src="src_file_path",
    #     dst=add_date_to_destination(src_file_path),
    #     bucket="homeworkbuckett",
    # )

    activateGCP
    # activateGCP > upload_file
# [END tutorial]
