"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
from __future__ import annotations

# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent
import json
import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, settings
from airflow.models import Connection

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# [END import_module]

def add_gcp_connection(**kwargs):
    new_conn = Connection(
            conn_id="google_cloud_default",
            conn_type='google_cloud_platform',
    )
    extra_field = {
        "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
        "extra__google_cloud_platform__project": "DataTempestHomework",
        "extra__google_cloud_platform__key_path": "/opt/airflow/dags/serviceaccount/datatempesthomework-7ab4cddb5cb8.json"
    }

    session = settings.Session()

    #checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        my_connection.set_extra(json.dumps(extra_field))
        session.add(my_connection)
        session.commit()
    else: #if it doesn't exit create one
        new_conn.set_extra(json.dumps(extra_field))
        session.add(new_conn)
        session.commit()

# [START instantiate_dag]
with DAG(

    dag_id = "sun_dag",
    # [START default_args]
    default_args={"owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 9),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
    },
    # [END default_args]

) as dag:
    # [END instantiate_dag]

    activateGCP = PythonOperator(
        task_id='add_gcp_connection_python',
        python_callable=add_gcp_connection,
        provide_context=True,
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src="/opt/airflow/data_source/inpatient_charges_2012.csv",
        dst="inpatient_charges_2012.csv",
        bucket="homeworkbuckett",
    )

    activateGCP >> upload_file
    # activateGCP > upload_file
# [END tutorial]
