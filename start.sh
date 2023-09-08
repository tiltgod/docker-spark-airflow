#! /bin/bash
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose -f docker-compose.Spark.yaml -f docker-compose.Airflow.yaml up -d