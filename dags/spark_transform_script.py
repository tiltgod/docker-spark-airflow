from pyspark.sql import SparkSession
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from create_gcp_bucket_obj import create_gcp_bucket_obj

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
bucket_client = create_gcp_bucket_obj(gcp_bucket_credential, gcp_bucket_name)
blobs = bucket_client.list_blobs(gcp_bucket_name)
dfs = []
for blob in blobs:
    with blob.open("r") as str_csv:
        csvStringIO = StringIO(str_csv.read())
        df = pd.read_csv(csvStringIO, sep=",", header=None)
        dfs.append(df)

# pyspark transform part
df_result = pd.concat(dfs)

# display df_result to bq
# create gcp BigQuery object
bq_client = bigquery.Client.from_service_account_info(gcp_bq_credential)
# dataset name
project_id = gcp_bq_credential["project_id"]
dest_dataset_name = "datatempest_dataset_test"
dataset_id = f"{project_id}.{dest_dataset_name}"
# table name
dest_table_name = "mytest1"
table_id = dataset_id + "." + dest_table_name

# check dataset and table exist
try:
    bq_client.get_dataset(dataset_id)  
    print("Dataset {} already exists".format(dataset_id))
    bq_client.get_dataset(table_id)  
    print("Dataset {} already exists".format(table_id))
except NotFound:
    print("Dataset {} is not found".format(dataset_id))
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-southeast1"
    dataset = bq_client.create_dataset(dataset, timeout=30)
    table = bigquery.Table(table_id)
    table = bq_client.create_table(table)

# load datafame to table
job_config = bigquery.LoadJobConfig(autodetect= True, source_format=bigquery.SourceFormat.CSV)
load_job = bq_client.load_table_from_dataframe(df_result, table_id, job_config=job_config)
table_results = bq_client.get_table(table_id)
