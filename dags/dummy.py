from airflow import DAG
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from tempest_homework_dag_config import default_config
from datetime import datetime
# [END import_module]


# [START instantiate_dag]
with DAG(

    dag_id = "dummy",
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


    spark_submit_local = SparkSubmitOperator(
            application ='/opt/airflow/dags/spark_etl_script_docker.py' ,
            conn_id= 'spark', 
            task_id='spark_submit_task', 
            dag=dag
            )



