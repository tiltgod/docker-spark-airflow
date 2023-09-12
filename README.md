# Data Tempest ETL homework
![photo_2023-08-25_17-10-38](https://github.com/tiltgod/docker-spark-airflow/assets/22901365/a0a52ddf-d1b4-4709-9210-9e1f867e87b6)

## building images

```
docker build -f Dockerfile.Spark . -t spark-air  
docker build -f Dockerfile.Airflow . -t airflow-spark
```

## starting 
```
bash start.sh
```
