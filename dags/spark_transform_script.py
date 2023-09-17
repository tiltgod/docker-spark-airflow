from pyspark.sql import SparkSession
from google.cloud import storage
from google.cloud import bigquery

import sys
from io import StringIO
import pandas as pd

# get application_args from dags
gcp_bucket_name = sys.argv[1]
gcp_bucket_credential = eval(sys.argv[2])
gcp_bq_credential = eval(sys.argv[3])

# start spark's session
spark = SparkSession.builder.appName('ConCatMeow').getOrCreate()

# list BLOB object form gcs bucket then put them in list
bucket_client = storage.Client.from_service_account_info(gcp_bucket_credential)
gcp_bucket_obj = storage.Bucket(bucket_client, gcp_bucket_name)
blobs = gcp_bucket_obj.list_blobs(gcp_bucket_name)

dfs = []
for blob in blobs:
    with blob.open("r") as str_csv:
        csvStringIO = StringIO(str_csv.read())
        df = pd.read_csv(csvStringIO, sep=",", header=None)
        dfs.append(df)

# pyspark transform part
df_result = pd.concat(dfs)

# BQ destination
table_id = "homework_table"
bq_client = bigquery.Client.from_service_account_info(gcp_bq_credential)
job_config = bigquery.LoadJobConfig(autodetect= True, source_format=bigquery.SourceFormat.CSV)
load_job = bq_client.load_table_from_dataframe(df_result, table_id, job_config=job_config)
table_results = bq_client.get_table(table_id)
print(table_results)

    