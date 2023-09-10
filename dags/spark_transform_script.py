from pyspark.sql import SparkSession
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import sys
from io import StringIO
import pandas as pd

gcp_bucket_name = sys.argv[1]
gcp_bucket_credential = eval(sys.argv[2])
gcp_bq_credential = eval(sys.argv[3])
spark = SparkSession.builder.appName('ConCatMeow').getOrCreate()

bucket_client = storage.Client.from_service_account_info(gcp_bucket_credential)
bucket = storage.Bucket(bucket_client, gcp_bucket_name)
blobs = bucket_client.list_blobs(gcp_bucket_name)

dfs = []
for blob in blobs:
    with blob.open("r") as str_csv:
        csvStringIO = StringIO(str_csv.read())
        df = pd.read_csv(csvStringIO, sep=",", header=None)
        dfs.append(df)

# concat
df_result = pd.concat(dfs)

# show df_result to bq

bq_client = bigquery.Client.from_service_account_info(gcp_bq_credential)
project_id = gcp_bq_credential["project_id"]
dataset_id = f"{project_id}.datatempest_dataset_test"
table_id = dataset_id + "." + "mytest1"

# check dataset exist
try:
    bq_client.get_dataset(dataset_id)  # Make an API request.
    print("Dataset {} already exists".format(dataset_id))
except NotFound:
    print("Dataset {} is not found".format(dataset_id))
    # create dataset
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-southeast1"
    dataset = bq_client.create_dataset(dataset, timeout=30)

# check table exist
try: 
    bq_client.get_table(table_id)
    print("Table {} already exists".format(table_id))
except NotFound:
    print("Table {} is not found".format(table_id))
    # create table
    table = bigquery.Table(table_id)
    table = bq_client.create_table(table)

# load datafame to table
job_config = bigquery.LoadJobConfig(autodetect= True, source_format=bigquery.SourceFormat.CSV)
load_job = bq_client.load_table_from_dataframe(df_result, table_id, job_config=job_config)
table_results = bq_client.get_table(table_id)