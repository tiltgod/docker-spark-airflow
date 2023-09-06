from pyspark.sql import SparkSession, Row
from google.cloud import storage
from pyspark.sql import DataFrame
from functools import reduce
from tempest_homework_dag_config import default_config

gcp_connection_config = default_config.gcp_connection_setting
spark = SparkSession.builder.appName('ConCatMeow').getOrCreate()

# example
# dept = [("Finance",10), 
#         ("Marketing",20), 
#         ("Sales",30), 
#         ("IT",40) 
#       ]

# deptColumns = ["dept_name","dept_id"]
# deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
# deptDF.printSchema()
# deptDF.show(truncate=False)
# example

# get files lst from bucket
path_to_private_key = gcp_connection_config.keypath
bucket_name = gcp_connection_config.bucket_name
client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
bucket = storage.Bucket(client, bucket_name)
str_folder_name_on_gcs = '/'
blobs = bucket.list_blobs(prefix=str_folder_name_on_gcs)
blobs_name = []
for blob in blobs:
    blobs_name.append(blob.name)

# read files
dfs = []
for file_name in blobs_name:
    df=spark.read.csv('/' + file_name, header=True)
    dfs.append(df)
# concat
df_result = reduce(DataFrame.unionAll, dfs)
df.result.show(truncate=False)
# show df_result to bq
# df.write.format("bigquery").mode("overwrite").option("temporaryGcsBucket", "censo-ensino-superior").option("database", "censo_ensino_superior") 
# database = dataset that created on BQ.option("table", "tablename").option("createDisposition", "CREATE_IF_NEEDED").save()



