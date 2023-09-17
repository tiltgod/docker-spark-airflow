from google.cloud import bigquery, exceptions
from typing import Any, Dict, List

class BigQueryWrapper():

    credential_info = {}
    client = None
    __wrapper = None

    @staticmethod
    def get_wrapper(credential_info):
        if BigQueryWrapper.__wrapper is None:
            BigQueryWrapper(credential_info)
        return BigQueryWrapper.__wrapper

    def __init__(self, credential_info):
        self.credential_info = credential_info
        self.location = "asia-southeast1"

        BigQueryWrapper.__wrapper = self
        
    def connect(self):
        self.client = bigquery.Client.from_service_account_info(self.credential_info)
        return self.client
            
    def load_from_df(self, dataframe, job_config, table_id):
        if dataframe.empty:
            raise ValueError('No DataFrame to upload.')
        try:
            job_config = bigquery.LoadJobConfig(autodetect= True, source_format=bigquery.SourceFormat.CSV)
            load_job = self.client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
        except Exception as e:
            raise Exception(f'Create BigQuery LoadJob failed, {e}')
        try:
            load_job.result()
        except exceptions.GoogleCloudError:
            err_list = load_job.errors
            err_msg = ' '.join([str(e) for e in err_list])
            raise Exception(f'LoadJob failed errors[] collection: {err_msg}')
        return load_job.output_rows
    
    def _get_full_table_id(self, dataset_id, table_id, project_id=None):
        if not project_id:
            return f"{self.client.project}.{dataset_id}.{table_id}"
        return f"{project_id}.{dataset_id}.{table_id}"
    
    @staticmethod
    def create_loadjob_config(schema_dict=None, source_format='CSV'):
        
        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
        )
        if schema_dict is not None:
            job_config.schema=BigQueryWrapper._convert_schema(schema_dict)
        else:
            job_config.autodetect=True
        return job_config

    @staticmethod
    def _convert_schema(schema_dict: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
        """list of dict to List[bigquery.SchemaField]"""
        schema = []
        for sch in schema_dict:
            try:
                api_repr_schema = BigQueryWrapper._transform_schema_dict_to_api_repr_format(schema=sch.copy())
                tmp_schema = bigquery.SchemaField.from_api_repr(api_repr_schema)
            except KeyError:
                print("Schema must contain keys [name, field_type]")
                raise
            schema.append(tmp_schema)
        return schema
        