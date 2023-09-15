#! /bin/bash
docker build -f Dockerfile.Spark . -t spark-air  
docker build -f Dockerfile.Airflow . -t airflow-spark