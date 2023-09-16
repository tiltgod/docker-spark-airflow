from dataclasses import dataclass
from data_class.gcp_connection_setting import GCPConnectionSettings
from data_class.to_gcs_setting import ToGCSSetting
from airflow.models import Variable

connection_config = Variable.get("gcp_connection", deserialize_json=True)
upload_config = Variable.get("upload_to_gcs", deserialize_json=True)

@dataclass
class DefaultSampleDag:
    gcp_connection_setting: GCPConnectionSettings
    to_gcs_setting: ToGCSSetting
    
default_config = DefaultSampleDag(
    gcp_connection_setting = GCPConnectionSettings(
        conn_id = connection_config["conn_id"],
        conn_type = connection_config["conn_type"],
        scope = connection_config["scope"],
        project = connection_config["project"],
        bucket_keypath = connection_config["bucket_keypath"],
        bq_keypath = connection_config["bq_keypath"]
    ),
    to_gcs_setting = ToGCSSetting(
        dir_name = upload_config["dir_name"],
        bucket_name = upload_config["bucket_name"]
    )
)