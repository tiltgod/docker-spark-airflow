from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql import DataFrame
from functools import reduce

spark = SparkSession.builder.appName('ConcatDFGCS').getOrCreate()


# get files lst from bucket
# path_to_private_key = '/'
# client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
# bucket = storage.Bucket(client, 'bucket-name')

# str_folder_name_on_gcs = '/'
# blobs = bucket.list_blobs(prefix=str_folder_name_on_gcs)
# blobs_name = []
# for blob in blobs:
#     blobs_name.append(blob.name)

# # read files
# dfs = []
# for file_name in blobs_name:
#     df=spark.read.csv('/' + file_name, header=True)
#     dfs.append(df)
# # concat
# df_result = reduce(DataFrame.unionAll, dfs)
# show df_result to bq
    # df.write\
    # .format("bigquery")\
    # .mode("overwrite")\
    # .option("temporaryGcsBucket", "censo-ensino-superior")\
    # .option("database", "censo_ensino_superior")\ # database = dataset that created on BQ
    # .option("table", "tablename")\
    # .option("createDisposition", "CREATE_IF_NEEDED")\
    # .save()



