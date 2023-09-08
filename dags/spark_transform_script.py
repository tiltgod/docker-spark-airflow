from pyspark.sql import SparkSession
from google.cloud import storage
# from pyspark.sql import DataFrame
# from functools import reduce
import sys
from io import StringIO
import pandas as pd

gcp_credential = eval(sys.argv[1])
gcp_bucket = sys.argv[2]

spark = SparkSession.builder.appName('ConCatMeow').getOrCreate()

client = storage.Client.from_service_account_info(gcp_credential)
bucket = storage.Bucket(client, gcp_bucket)
str_folder_name_on_gcs = '/'
blobs = bucket.list_blobs(prefix=str_folder_name_on_gcs)

dfs = []
for blob in blobs:
    with open(blob) as str_csv:
        csvStringIO = StringIO(str_csv.read())
        df = pd.read_csv(csvStringIO, sep=",", header=None)
        dfs.append(df
                   )

# concat
df_result = pd.concat(dfs)
print(df_result)

# show df_result to bq
