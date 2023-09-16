from pyspark.sql import SparkSession
from connector.gcs import GCSWrapper
from connector.bq import BigQueryWrapper
from google.cloud.exceptions import NotFound

import sys
from io import StringIO
import pandas as pd

# get application_args from dags
gcp_bucket_name = sys.argv[1]
gcp_bucket_credential = eval(sys.argv[2])
gcp_bq_credential = eval(sys.argv[3])

# start spark's session
spark = SparkSession.builder.appName('ConCatMeow').getOrCreate()

# create connectoin
gcs_client = GCSWrapper.getWrapper(gcp_bucket_credential)
gcs_client.connect()
# list BLOB object form gcs bucket then put them in list
blobs = gcs_client.get_blob_list(gcp_bucket_name)

# pyspark transform part
dfs = []
for blob in blobs:
    with blob.open("r") as str_csv:
        csvStringIO = StringIO(str_csv.read())
        df = pd.read_csv(csvStringIO, sep=",", header=None)
        dfs.append(df)
df_result = pd.concat(dfs)

# display df_result to bq
# create gcp BigQuery object
bq_client = BigQueryWrapper.getWrapper(gcp_bq_credential)
bq_client.connect()

# load dataframe to bq table

# job_config = bigquery.LoadJobConfig(autodetect= True, source_format=bigquery.SourceFormat.CSV)
# load_job = bq_client.load_table_from_dataframe(df_result, table_id, job_config=job_config)
# table_results = bq_client.get_table(table_id)
