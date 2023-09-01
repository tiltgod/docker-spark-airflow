# Data Tempest ETL homework
![photo_2023-08-25_17-10-38](https://github.com/tiltgod/docker-spark-airflow/assets/22901365/a0a52ddf-d1b4-4709-9210-9e1f867e87b6)

git source:
https://github.com/yTek01/docker-spark-airflow

example:
https://medium.com/google-cloud/apache-airflow-how-to-add-a-connection-to-google-cloud-with-cli-af2cc8df138d

gcp_connection ariflow docker:
https://airflow.apache.org/docs/apache-airflow/1.10.3/howto/connection/gcp.html
https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/connection/index.html

docker exec -it <spark-worker-1container_id> spark-submit --master spark://ad13c1d12cf0:7077  spark_etl_script_docker.py


# exec to the spark master node , and send spark-submit
# docker exec -u 0 -it becd3addecbf /bin/bash (run as root user)
docker exec -it 7ceb98a2af31 spark-submit --master spark://7ceb98a2af31:7077  ./dags/spark_etl_script_docker.py

# check network of container
docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" 871fe9923dc5
 7c3b37b7aa73
871fe9923dc5

# install ping (used to check IP Adress of your container)

apt-get update -y
apt-get install -y iputils-ping
ping <service_name>