from pyspark.sql import SparkSession

def spark_session():
    spark = SparkSession.builder\
            .master("local")\
            .appName("VSCode")\
            .config('spark.ui.port', '8090')\
            .getOrCreate()
    return spark

spark = SparkSession.builder\
        .master("local")\
        .appName("VSCode")\
        .config('spark.ui.port', '8090')\
        .getOrCreate()

df = spark.read.csv("/opt/airflow/tmp/electricity_access_percent.csv")
df.show()